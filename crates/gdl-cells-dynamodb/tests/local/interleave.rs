//! Every order of the calls of a cell's writers, with TTL and a late
//! attempt of a write: after each call, the list a cell's stored record
//! names is there and is not set to expire, and no two writers put the
//! same list item.
//!
//! Each writer's client holds every DynamoDB call until the schedule lets
//! it run, so an order is a sequence of choices of which writer's next
//! call runs. The orders are explored depth first: each one runs in a new
//! cell of the same tables, replaying the choices before the one it
//! changes. A writer can stop early, refused as not newer, so the orders
//! come from the runs, not from a count of calls made in advance.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::JoinHandle,
};

use aws_sdk_dynamodb::{
    Client,
    config::{
        ConfigBag, Intercept, RuntimeComponents,
        interceptors::{
            BeforeDeserializationInterceptorContextRef, BeforeSerializationInterceptorContextRef,
            FinalizerInterceptorContextRef,
        },
        retry::RetryConfig,
    },
    operation::{put_item::PutItemInput, update_item::UpdateItemInput},
    types::AttributeValue,
};
use gdl_cells::{CellCommand, CellDirectory, CellRecord, CommandWrite, DirectoryError, Withdrawal};
use gdl_cells_dynamodb::{DynamoDirectory, Tables};

use super::{
    LIMITS, MODEL, WAIT, client, lists, named_list, own_pool, record, snapshot, tables, watch,
};

/// What a writer's client tells the schedule.
#[derive(Debug)]
enum Said {
    /// A call waits to run.
    Waits(Call),
    /// The call that ran has finished.
    Ran,
    /// The writer has made its last call.
    Done,
}

/// A call to DynamoDB, as the schedule reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    List(String),
    Record,
    /// A records-table put that keeps the handle's list: it holds only
    /// while the cell's item names that list.
    Kept,
    Expire,
    Other,
}

/// What a schedule's choices reached, so that a variant can say which
/// branches of a write it must reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Branch {
    /// A write marked a list to expire.
    Expiry,
    /// A write was refused as not newer.
    NotNewer,
    /// A write that kept its list was refused for the list the cell's
    /// item named, and put a list of its own.
    Fallback,
    /// A records-table put landed, and its answer was lost.
    Lost,
    /// A late attempt of a records-table put was refused.
    LateRefused,
}

/// Holds each call of one writer's client until the schedule lets it run.
#[derive(Debug)]
struct Gate {
    writer: &'static str,
    said: Sender<Said>,
    go: Mutex<Receiver<()>>,
    // Each lists-table put, by writer and list key.
    puts: Arc<Mutex<Vec<(&'static str, String)>>>,
    // Whether the call under way is a records-table put.
    record: AtomicBool,
    // Lose the answer of this writer's first records-table put.
    lose: AtomicBool,
    lost: Arc<AtomicBool>,
    // The first records-table put of this writer, kept for a late attempt.
    first: Option<Arc<Mutex<Option<PutItemInput>>>>,
}

impl Intercept for Gate {
    fn name(&self) -> &'static str {
        "Gate"
    }

    fn read_before_execution(
        &self,
        context: &BeforeSerializationInterceptorContextRef<'_>,
        _config: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let input = context.input();
        let call = if let Some(put) = input.downcast_ref::<PutItemInput>() {
            let item = put.item().cloned().unwrap_or_default();
            match item.get("list") {
                Some(AttributeValue::S(key)) if item.contains_key("owner") => {
                    self.puts
                        .lock()
                        .expect("the puts")
                        .push((self.writer, key.clone()));
                    Call::List(key.clone())
                }
                _ => {
                    if let Some(first) = &self.first {
                        first
                            .lock()
                            .expect("the first put")
                            .get_or_insert_with(|| put.clone());
                    }
                    if put
                        .condition_expression()
                        .is_some_and(|condition| condition.contains("#names"))
                    {
                        Call::Kept
                    } else {
                        Call::Record
                    }
                }
            }
        } else if input.downcast_ref::<UpdateItemInput>().is_some() {
            Call::Expire
        } else {
            Call::Other
        };
        self.record
            .store(matches!(call, Call::Record | Call::Kept), Ordering::Relaxed);
        self.said
            .send(Said::Waits(call))
            .expect("the schedule hears its writers");
        self.go
            .lock()
            .expect("the gate")
            .recv_timeout(WAIT)
            .expect("the schedule runs every call it holds");
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _components: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if context.response().status().is_success()
            && self.record.load(Ordering::Relaxed)
            && self.lose.swap(false, Ordering::Relaxed)
        {
            self.lost.store(true, Ordering::Relaxed);
            return Err(std::io::Error::other("the answer of a stored record was lost").into());
        }
        Ok(())
    }

    fn read_after_execution(
        &self,
        _context: &FinalizerInterceptorContextRef<'_>,
        _components: &RuntimeComponents,
        _config: &mut ConfigBag,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.said
            .send(Said::Ran)
            .expect("the schedule hears its writers");
        Ok(())
    }
}

/// A writer of a variant: its name, its writes in order, whether the
/// answer of its first records-table put is lost, and whether that put is
/// sent again late.
struct Plan {
    name: &'static str,
    writes: Vec<CommandWrite>,
    lose: bool,
    late: bool,
}

/// One writer of a run: its thread, and the two ends of its gate.
struct Writer {
    name: &'static str,
    said: Receiver<Said>,
    go: Sender<()>,
    thread: Option<JoinHandle<Vec<Result<(), DirectoryError>>>>,
    waits: Option<Call>,
    calls: Vec<Call>,
    lost: Arc<AtomicBool>,
    first: Option<Arc<Mutex<Option<PutItemInput>>>>,
}

impl Writer {
    /// Start `plan`'s writer, which publishes its writes in order through
    /// a handle of its own, each call held by its gate. Its client does
    /// not retry, so that each call is one attempt.
    fn start(
        plan: Plan,
        client: &Client,
        tables: &Tables,
        puts: &Arc<Mutex<Vec<(&'static str, String)>>>,
    ) -> Self {
        let (said, hears) = mpsc::channel();
        let (go, gate) = mpsc::channel();
        let lost = Arc::new(AtomicBool::new(false));
        let first = plan.late.then(Arc::default);
        let config = own_pool(client)
            .retry_config(RetryConfig::disabled())
            .interceptor(Gate {
                writer: plan.name,
                said: said.clone(),
                go: Mutex::new(gate),
                puts: Arc::clone(puts),
                record: AtomicBool::new(false),
                lose: AtomicBool::new(plan.lose),
                lost: Arc::clone(&lost),
                first: first.clone(),
            })
            .build();
        let directory =
            DynamoDirectory::with_limits(Client::from_conf(config), tables.clone(), LIMITS);
        let writes = plan.writes;
        let thread = std::thread::spawn(move || {
            let results = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the writer's runtime")
                .block_on(async move {
                    let mut results = Vec::new();
                    for write in writes {
                        results.push(write.to(&directory).await);
                    }
                    results
                });
            said.send(Said::Done)
                .expect("the schedule hears its writers");
            results
        });
        let mut writer = Self {
            name: plan.name,
            said: hears,
            go,
            thread: Some(thread),
            waits: None,
            calls: Vec::new(),
            lost,
            first,
        };
        writer.hear();
        writer
    }

    /// Wait for the writer's next call, or its end.
    fn hear(&mut self) {
        match self
            .said
            .recv_timeout(WAIT)
            .expect("a writer moves within the bound")
        {
            Said::Waits(call) => self.waits = Some(call),
            Said::Done => self.waits = None,
            Said::Ran => panic!("{}: a call finished that did not run", self.name),
        }
    }

    /// Run the writer's waiting call to its end.
    fn step(&mut self) -> String {
        let call = self.waits.take().expect("a waiting call");
        self.go.send(()).expect("the writer waits");
        match self
            .said
            .recv_timeout(WAIT)
            .expect("a call ends within the bound")
        {
            Said::Ran => {}
            other => panic!("{}: expected the end of {call:?}, got {other:?}", self.name),
        }
        self.calls.push(call.clone());
        self.hear();
        format!("{}: {call:?}", self.name)
    }

    /// The first records-table put, once the writer has made it.
    fn first(&self) -> Option<PutItemInput> {
        self.first
            .as_ref()?
            .lock()
            .expect("the first put")
            .clone()
            .filter(|_| {
                self.calls
                    .iter()
                    .any(|call| matches!(call, Call::Record | Call::Kept))
            })
    }

    fn join(mut self) -> (Vec<Call>, bool, Vec<Result<(), DirectoryError>>) {
        let results = self
            .thread
            .take()
            .expect("the writer's thread")
            .join()
            .expect("the writer's thread ends");
        (self.calls, self.lost.load(Ordering::Relaxed), results)
    }
}

/// A choice of the schedule: a writer's next call, TTL, or the late
/// attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Choice {
    Writer(usize),
    Ttl,
    Late(usize),
}

/// Delete each list of `cell` that is set to expire, as TTL does once its
/// hour has passed.
async fn ttl(client: &Client, tables: &Tables, cell: &str) {
    for (key, expiry) in lists(client, tables, cell).await {
        let Some(expiry) = expiry else { continue };
        client
            .delete_item()
            .table_name(&tables.lists)
            .key("owner", AttributeValue::S(format!("{MODEL}/{cell}")))
            .key("list", AttributeValue::S(key))
            .condition_expression("#expires = :expires")
            .expression_attribute_names("#expires", "expires_at")
            .expression_attribute_values(":expires", AttributeValue::N(expiry.to_string()))
            .send()
            .await
            .expect("TTL removes a list set to expire");
    }
}

/// Send `put` again, as a late attempt of it would arrive: whether the
/// table took it.
async fn late(client: &Client, put: &PutItemInput) -> bool {
    let sent = client
        .put_item()
        .set_table_name(put.table_name().map(str::to_owned))
        .set_item(put.item().cloned())
        .set_condition_expression(put.condition_expression().map(str::to_owned))
        .set_expression_attribute_names(put.expression_attribute_names().cloned())
        .set_expression_attribute_values(put.expression_attribute_values().cloned())
        .send()
        .await;
    match sent {
        Ok(_) => true,
        Err(error)
            if error
                .as_service_error()
                .is_some_and(|error| error.is_conditional_check_failed_exception()) =>
        {
            false
        }
        Err(error) => panic!("a late attempt: {error:?}"),
    }
}

/// The cell's stored item is a record whose list is there and not set to
/// expire, or a withdrawal.
async fn live(client: &Client, tables: &Tables, cell: &str) -> Result<(), String> {
    let stored = client
        .get_item()
        .table_name(&tables.records)
        .key("class", AttributeValue::S(MODEL.into()))
        .key("cell", AttributeValue::S(cell.into()))
        .consistent_read(true)
        .send()
        .await
        .expect("the stored item");
    let withdrawn = stored
        .item()
        .and_then(|item| item.get("kind"))
        .is_some_and(|kind| *kind == AttributeValue::S("withdrawn".into()));
    if withdrawn {
        return Ok(());
    }
    let (key, expiry) = named_list(client, tables, cell).await?;
    expiry.map_or(Ok(()), |at| Err(format!("{key} is set to expire at {at}")))
}

/// A variant: its writers, given the cell's first record, whether TTL runs
/// in its orders, and the branches its orders must reach.
struct Variant {
    name: &'static str,
    writers: fn(&CellRecord) -> Vec<Plan>,
    ttl: bool,
    reaches: &'static [Branch],
}

fn plan(name: &'static str, writes: Vec<CommandWrite>) -> Plan {
    Plan {
        name,
        writes,
        lose: false,
        late: false,
    }
}

fn at(first: &CellRecord, epoch: u64, seq: u64) -> CommandWrite {
    CommandWrite::Record(Box::new(CellRecord {
        epoch,
        seq,
        ..first.clone()
    }))
}

/// `first`'s cell at a higher `models_version`.
fn higher(first: &CellRecord, epoch: u64, seq: u64) -> CommandWrite {
    CommandWrite::Record(Box::new(CellRecord {
        epoch,
        seq,
        ..record(&first.name, 1, 2, &["qwen-32b", "llama-70b"])
    }))
}

/// The command, and a hub's writes at `hub_epoch`: one at the first
/// record's `models_version`, `heartbeats` more of the same list, and one
/// at a higher `models_version`, which expires the list it moves past.
fn command_and_hub(
    first: &CellRecord,
    hub_epoch: impl Fn(&CommandWrite) -> u64,
    heartbeats: u64,
) -> [Plan; 2] {
    let command = CellCommand::ScaleToZero.write(first, std::time::SystemTime::now());
    let epoch = hub_epoch(&command);
    let mut hub: Vec<CommandWrite> = (1..=1 + heartbeats)
        .map(|seq| at(first, epoch, seq))
        .collect();
    hub.push(higher(first, epoch, 2 + heartbeats));
    [plan("command", vec![command]), plan("hub", hub)]
}

/// Run every order of `variant`, each in a cell of its own: the number of
/// orders, and the branches they reached.
async fn explore(client: &Client, tables: &Tables, variant: &Variant) -> (usize, BTreeSet<Branch>) {
    let mut prefix: Vec<Choice> = Vec::new();
    let mut orders = 0;
    let mut reached = BTreeSet::new();
    loop {
        let cell = format!("{}-{orders}", variant.name);
        let trace = run(client, tables, variant, &cell, &prefix, &mut reached).await;
        orders += 1;
        // The deepest choice with an untried alternative: change it, and
        // keep the choices before it.
        let Some(depth) = trace
            .iter()
            .rposition(|(enabled, chosen)| chosen + 1 < enabled.len())
        else {
            return (orders, reached);
        };
        prefix = trace[..depth]
            .iter()
            .map(|(enabled, chosen)| enabled[*chosen])
            .collect();
        let (enabled, chosen) = &trace[depth];
        prefix.push(enabled[chosen + 1]);
    }
}

/// Run one order in `cell`: the choices of `prefix`, then the first one
/// enabled at each later step. The choices enabled at each step, and the
/// one taken.
async fn run(
    client: &Client,
    tables: &Tables,
    variant: &Variant,
    cell: &str,
    prefix: &[Choice],
    reached: &mut BTreeSet<Branch>,
) -> Vec<(Vec<Choice>, usize)> {
    let first = record(cell, 1, 1, &["qwen-32b"]);
    DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS)
        .publish(first.clone())
        .await
        .expect("the cell's first record");
    let puts = Arc::new(Mutex::new(Vec::new()));
    let mut writers: Vec<Writer> = (variant.writers)(&first)
        .into_iter()
        .map(|plan| Writer::start(plan, client, tables, &puts))
        .collect();
    let mut ttl_owed = variant.ttl;
    let mut late_owed = true;
    let mut trace = Vec::new();
    let mut steps = Vec::new();
    loop {
        let mut enabled: Vec<Choice> = writers
            .iter()
            .enumerate()
            .filter(|(_, writer)| writer.waits.is_some())
            .map(|(n, _)| Choice::Writer(n))
            .collect();
        if ttl_owed {
            enabled.push(Choice::Ttl);
        }
        if late_owed && let Some(n) = writers.iter().position(|writer| writer.first().is_some()) {
            enabled.push(Choice::Late(n));
        }
        if enabled.is_empty() {
            break;
        }
        let choice = prefix.get(trace.len()).copied().unwrap_or(enabled[0]);
        let chosen = enabled
            .iter()
            .position(|enabled| *enabled == choice)
            .unwrap_or_else(|| {
                panic!("{cell}: a replay diverged at {steps:?}: {choice:?} is not in {enabled:?}")
            });
        trace.push((enabled, chosen));
        match choice {
            Choice::Writer(n) => steps.push(writers[n].step()),
            Choice::Ttl => {
                ttl(client, tables, cell).await;
                ttl_owed = false;
                steps.push("TTL".to_owned());
            }
            Choice::Late(n) => {
                let put = writers[n].first().expect("a first put");
                let taken = late(client, &put).await;
                if !taken {
                    reached.insert(Branch::LateRefused);
                }
                late_owed = false;
                steps.push(format!(
                    "late attempt of {}'s first record: taken={taken}",
                    writers[n].name
                ));
            }
        }
        // The property holds after every call, not only at the end.
        if let Err(broken) = live(client, tables, cell).await {
            panic!(
                "{cell}: the stored record's list is not live after {steps:#?}: {broken}; lists: {:?}",
                lists(client, tables, cell).await
            );
        }
    }
    let joined: Vec<_> = writers
        .into_iter()
        .map(|writer| (writer.name, writer.join()))
        .collect();
    for (_, (calls, lost, results)) in &joined {
        if calls.contains(&Call::Expire) {
            reached.insert(Branch::Expiry);
        }
        if calls
            .windows(2)
            .any(|pair| pair[0] == Call::Kept && matches!(pair[1], Call::List(_)))
        {
            reached.insert(Branch::Fallback);
        }
        if *lost {
            reached.insert(Branch::Lost);
        }
        if results
            .iter()
            .any(|result| matches!(result, Err(DirectoryError::NotNewer { .. })))
        {
            reached.insert(Branch::NotNewer);
        }
    }
    let puts = puts.lock().expect("the puts");
    let mut by_key: HashMap<&str, &str> = HashMap::new();
    for (writer, key) in puts.iter() {
        if let Some(other) = by_key.insert(key, writer).filter(|other| other != writer) {
            panic!(
                "{cell}: {other} and {writer} both put list {key}, so one put replaced the other's, after {steps:#?}"
            );
        }
    }
    trace
}

const VARIANTS: [Variant; 6] = [
    // The hub's epoch is below the command's, as a hub that restarted
    // while the command was on its way.
    Variant {
        name: "below",
        writers: |first| command_and_hub(first, |_| 2, 0).into(),
        ttl: true,
        reaches: &[Branch::Expiry, Branch::NotNewer],
    },
    // A hub that restarted after the command, as gdl's hub takes an epoch
    // past the records it finds.
    Variant {
        name: "above",
        writers: |first| command_and_hub(first, |command| command.version().epoch + 1, 0).into(),
        ttl: true,
        reaches: &[Branch::Expiry, Branch::NotNewer],
    },
    // A heartbeat between the hub's two writes keeps its list, and after
    // the command's record puts a list of its own.
    Variant {
        name: "heartbeat",
        writers: |first| command_and_hub(first, |command| command.version().epoch + 1, 1).into(),
        ttl: true,
        reaches: &[Branch::Expiry, Branch::Fallback],
    },
    // The answer of the hub's first record is lost, so its heartbeat puts
    // the same list again with a new token; that record put is sent again
    // late, anywhere after it. TTL is left out: the property is checked
    // after every call, so a list set to expire that a record names fails
    // before TTL could remove it.
    Variant {
        name: "lost",
        writers: |first| {
            let [command, hub] = command_and_hub(first, |_| 2, 1);
            vec![
                command,
                Plan {
                    lose: true,
                    late: true,
                    ..hub
                },
            ]
        },
        ttl: false,
        reaches: &[Branch::Expiry, Branch::Lost, Branch::LateRefused],
    },
    // Two handles at one epoch: the first keeps its list, and the second
    // writes between at a higher models_version, which expires that list.
    Variant {
        name: "epoch-models",
        writers: |first| {
            vec![
                plan("a", vec![at(first, 2, 1), at(first, 2, 3)]),
                plan("b", vec![higher(first, 2, 2)]),
            ]
        },
        ttl: true,
        reaches: &[Branch::Expiry, Branch::Fallback],
    },
    // The same, with a withdrawal between, which expires nothing.
    Variant {
        name: "epoch-withdrawal",
        writers: |first| {
            vec![
                plan("a", vec![at(first, 2, 1), at(first, 2, 3)]),
                plan(
                    "b",
                    vec![CommandWrite::Withdrawal(Withdrawal {
                        class: first.class.clone(),
                        cell: first.name.clone(),
                        epoch: 2,
                        seq: 2,
                    })],
                ),
            ]
        },
        ttl: true,
        reaches: &[Branch::Fallback],
    },
];

// About a thousand orders, each a few calls to DynamoDB Local, so this
// case runs for many seconds: the time is in the orders, not in waits.
// The variants run at once, each on a thread and a client of its own,
// since the schedule blocks its thread while a call runs.
#[tokio::test]
async fn every_order_of_a_cells_writers_and_ttl_leaves_a_live_list() {
    let client = client();
    let tables = tables(&client).await;
    std::thread::scope(|scope| {
        for variant in &VARIANTS {
            let (client, tables) = (Client::from_conf(own_pool(&client).build()), &tables);
            scope.spawn(move || {
                let (orders, reached) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("the variant's runtime")
                    .block_on(explore(&client, tables, variant));
                eprintln!("{}: {orders} orders, reached {reached:?}", variant.name);
                for branch in variant.reaches {
                    assert!(
                        reached.contains(branch),
                        "{}: no order reached {branch:?}: {reached:?}",
                        variant.name
                    );
                }
            });
        }
    });
    // A reader joins every stored record to its list.
    let directory = DynamoDirectory::with_limits(client.clone(), tables.clone(), LIMITS);
    let (records, _) = snapshot(&mut watch(&directory, None)).await;
    assert!(
        records.iter().all(|record| !record.models.is_empty()),
        "every record is joined to its list: {records:?}"
    );
}
