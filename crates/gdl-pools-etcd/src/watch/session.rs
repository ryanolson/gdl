//! A run's setup: a connection to one member, the class's snapshot when
//! the class has none, and the store's watch, trusted only once it is
//! created and the store is the one the view was read from.

use std::{collections::HashMap, sync::Arc};

use etcd_client::{Client, GetOptions, SortOrder, SortTarget, WatchOptions, WatchStream};
use gdl_pools::DirectoryError;

use super::{Ended, Entry, KEY_OVERHEAD, View};
use crate::{
    directory::{self, Shared, call, identify, kv, read_identity},
    value::{self, Name, Stored},
};

/// Connect to `endpoint`, read the class's snapshot if its view has none,
/// and watch the class from the revision after the view's: the client, the
/// trusted watch, and the view's revision.
pub(super) async fn open(
    shared: &Arc<Shared>,
    name: &str,
    class_prefix: &str,
    generation: u64,
    endpoint: usize,
) -> Result<(Client, WatchStream, i64), Ended> {
    let broken = |error| {
        Err(Ended::Broken {
            error,
            trusted: false,
        })
    };
    let limits = shared.limits;
    let Some((cursor, id)) = shared.with(name, generation, |class| {
        (class.view.cursor, class.view.id.clone())
    }) else {
        return Err(Ended::Stopped);
    };
    // A client of one member, so that the watch and the identity it is
    // checked by are read from one store.
    let client = match directory::connect(&shared.endpoints[endpoint..=endpoint]).await {
        Ok(client) => client,
        Err(error) => return Err(Ended::Failed(error)),
    };
    let (cursor, id) = match (cursor, id) {
        (cursor, Some(id)) if cursor > 0 => (cursor, id),
        _ => {
            let id = match identify(&client, &limits).await {
                Ok(id) => id,
                Err(error) => return broken(error),
            };
            let view = match read_snapshot(shared, &client, name, class_prefix).await {
                Ok(view) => view,
                Err(Ended::Broken { error, .. }) => return broken(error),
                Err(ended) => return Err(ended),
            };
            let cursor = view.cursor;
            let view = View {
                id: Some(id.clone()),
                ..view
            };
            if shared
                .with(name, generation, |class| class.install(view))
                .is_none()
            {
                return Err(Ended::Stopped);
            }
            (cursor, id)
        }
    };
    #[cfg(feature = "etcd-local")]
    shared.hook(crate::testing::Point::BeforeWatch).await;
    let mut watches = client
        .watch_client()
        .max_decoding_message_size(limits.response_bytes);
    let options = WatchOptions::new()
        .with_prefix()
        .with_start_revision(cursor + 1)
        .with_fragment();
    let what = format!("watching class `{name}`");
    let mut stream = match call(
        &what,
        limits.timeout,
        watches.watch(class_prefix, Some(options)),
    )
    .await
    {
        Ok(stream) => stream,
        Err(error) => return broken(error),
    };
    let created = match tokio::time::timeout(limits.timeout, stream.message()).await {
        Ok(Ok(Some(created))) if created.created() => created,
        Ok(Ok(Some(_))) => {
            return broken(DirectoryError::Unavailable(format!(
                "{what}: the first response did not create the watch"
            )));
        }
        Ok(Ok(None)) => {
            return broken(DirectoryError::Unavailable(format!(
                "{what}: the watch ended"
            )));
        }
        Ok(Err(error)) => return broken(DirectoryError::Unavailable(format!("{what}: {error}"))),
        Err(_) => {
            return broken(DirectoryError::Unavailable(format!(
                "{what}: etcd did not create the watch within {:?}",
                limits.timeout
            )));
        }
    };
    // A store whose revision is below the view's is not the store the
    // view was read from: an empty one reuses small revisions, and would
    // skip every write below the view's.
    let at = created.header().map_or(0, |header| header.revision());
    if at < cursor {
        return Err(Ended::Resnapshot(format!(
            "the store is at revision {at}, below this watch's {cursor}: it was replaced"
        )));
    }
    // Read once the watch exists, on its connection: an identity checked
    // before would leave the store free to be replaced between the check
    // and the watch.
    match read_identity(&client, &limits).await {
        Ok(Some(now)) if now == id => {}
        Ok(_) => {
            return Err(Ended::Resnapshot(
                "the store's identity is not the one this watch read: it was replaced".into(),
            ));
        }
        Err(error) => return broken(error),
    }
    Ok((client, stream, cursor))
}

/// The newest revision of any key of the class, by a read now; 0 for a
/// class with no key.
pub(super) async fn newest(
    client: &Client,
    class_prefix: &str,
    limits: &directory::EtcdLimits,
) -> Result<i64, DirectoryError> {
    let options = GetOptions::new()
        .with_prefix()
        .with_sort(SortTarget::Mod, SortOrder::Descend)
        .with_limit(1)
        .with_keys_only();
    let got = call(
        "reading a class's newest key",
        limits.timeout,
        kv(client, limits).get(class_prefix, Some(options)),
    )
    .await?;
    Ok(got.kvs().first().map_or(0, |kv| kv.mod_revision()))
}

/// The class's keys at one revision, in pages of at most the response
/// limit's bytes, read again from a new revision if compaction passes the
/// first while the pages are read; the view they make.
async fn read_snapshot(
    shared: &Shared,
    client: &Client,
    name: &str,
    class_prefix: &str,
) -> Result<View, Ended> {
    let limits = shared.limits;
    let end = value::range_end(class_prefix.as_bytes());
    // A page holds at most this many keys of the largest value etcd takes.
    let largest = limits.request_bytes + class_prefix.len() + KEY_OVERHEAD;
    let mut page = (limits.response_bytes / largest).max(1);
    let mut kv = kv(client, &limits);
    let what = format!("reading class `{name}`");
    'attempt: loop {
        let mut revision = 0;
        let mut start = class_prefix.as_bytes().to_vec();
        let mut kvs = Vec::new();
        loop {
            if revision > 0 {
                #[cfg(feature = "etcd-local")]
                shared.hook(crate::testing::Point::BetweenPages).await;
            }
            let options = GetOptions::new()
                .with_range(end.clone())
                .with_limit(i64::try_from(page).unwrap_or(i64::MAX))
                .with_revision(revision);
            let got =
                tokio::time::timeout(limits.timeout, kv.get(start.clone(), Some(options))).await;
            let mut got = match got {
                Ok(Ok(got)) => got,
                Ok(Err(etcd_client::Error::GRpcStatus(status)))
                    if status.message().contains("compacted") =>
                {
                    tracing::debug!(
                        class = name,
                        revision,
                        "a snapshot's revision was compacted while it was read: reading again"
                    );
                    continue 'attempt;
                }
                Ok(Err(etcd_client::Error::GRpcStatus(status)))
                    if status.message().contains("too large") =>
                {
                    if page == 1 {
                        return Err(Ended::Failed(DirectoryError::Unavailable(format!(
                            "{what}: one key is larger than the response limit, {} bytes: {}",
                            limits.response_bytes,
                            status.message()
                        ))));
                    }
                    page /= 2;
                    continue;
                }
                Ok(Err(error)) => {
                    return Err(Ended::Broken {
                        error: DirectoryError::Unavailable(format!("{what}: {error}")),
                        trusted: false,
                    });
                }
                Err(_) => {
                    return Err(Ended::Broken {
                        error: DirectoryError::Unavailable(format!(
                            "{what}: etcd did not answer within {:?}",
                            limits.timeout
                        )),
                        trusted: false,
                    });
                }
            };
            if revision == 0 {
                revision = got.header().map_or(0, |header| header.revision());
            }
            let more = got.more();
            let read = got.take_kvs();
            if let Some(last) = read.last() {
                start = [last.key(), &[0]].concat();
            }
            kvs.extend(read);
            if !more {
                break;
            }
        }
        return Ok(view_of(name, class_prefix, revision, kvs));
    }
}

/// The view the keys `kvs` of the class, read at `revision`, make: each
/// record joined to the list it names.
fn view_of(name: &str, class_prefix: &str, revision: i64, kvs: Vec<etcd_client::KeyValue>) -> View {
    let mut records = Vec::new();
    let mut lists = HashMap::new();
    for kv in &kvs {
        match value::name(class_prefix, kv.key()) {
            Some(Name::List(pool)) => {
                lists.insert(pool.to_owned(), (kv.mod_revision(), kv.value().to_vec()));
            }
            Some(Name::Record(pool)) => match value::decode(kv.value()) {
                Some(stored) => records.push((pool.to_owned(), kv.mod_revision(), stored)),
                None => tracing::warn!(
                    class = name,
                    pool,
                    "a record key holds a value this backend did not write: skipped"
                ),
            },
            None => {}
        }
    }
    let mut view = View {
        cursor: revision.max(1),
        ..View::default()
    };
    for (pool, at, stored) in records {
        match stored {
            Stored::Withdrawn(_) => {
                view.entries.insert(pool, Entry::Withdrawn);
            }
            Stored::Record { fields, list, .. } => {
                let named = list.unwrap_or(at);
                let joined = lists
                    .get(&pool)
                    .filter(|(revision, _)| *revision == named)
                    .and_then(|(_, list)| value::join(&fields, list));
                match joined {
                    Some(record) => {
                        view.entries.insert(pool, Entry::Record(Arc::new(record)));
                    }
                    None => {
                        tracing::warn!(
                            class = name,
                            pool,
                            "a record names a property list its list key does not hold: it waits for that list"
                        );
                        view.pending.insert(pool, (fields, named));
                    }
                }
            }
        }
    }
    view.lists = lists;
    view
}
