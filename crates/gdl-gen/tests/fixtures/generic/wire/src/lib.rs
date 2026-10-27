//! Serde impls written for one instantiation of a generic message, which
//! make that instantiation a wire type and no other; and the parameters
//! serde's derive bounds, which are the ones an argument must be a wire
//! type for.

use std::marker::PhantomData;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Serde's traits for `Narrow<u8>` alone.
#[derive(gdl_message::Message)]
pub struct Narrow<T>(pub T);

impl Serialize for Narrow<u8> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Narrow<u8> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u8::deserialize(deserializer).map(Self)
    }
}

/// The instantiation the impls are for.
#[gdl_message::export]
pub type ByteNarrow = Narrow<u8>;

/// One they are not for.
#[gdl_message::export]
pub type WordNarrow = Narrow<u16>;

/// `Serialize` for one instantiation and `Deserialize` for another: each
/// has one of the two.
#[derive(gdl_message::Message)]
pub struct Crossed<T>(pub T);

impl Serialize for Crossed<u8> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Crossed<u16> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u16::deserialize(deserializer).map(Self)
    }
}

/// Serializes only.
#[gdl_message::export]
pub type ByteCrossed = Crossed<u8>;

/// Deserializes only.
#[gdl_message::export]
pub type WordCrossed = Crossed<u16>;

/// A carrier whose serde derive bounds its argument: a wire type when the
/// instantiation it carries is one.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Outer<T> {
    /// What it carries.
    pub inner: T,
}

/// It carries the instantiation the impls are for.
#[gdl_message::export]
pub type ByteOuter = Outer<Narrow<u8>>;

/// It carries one they are not for.
#[gdl_message::export]
pub type WordOuter = Outer<Narrow<u16>>;

/// A type without serde's traits.
pub struct Opaque;

/// A carrier whose builder is a marker: serde bounds no parameter that
/// only `PhantomData` names.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Marked<P, B> {
    /// What it carries.
    pub payload: P,
    /// Which builder, as a type alone.
    pub builder: PhantomData<fn() -> B>,
}

/// A wire type, though its builder is not one.
#[gdl_message::export]
pub type OpaqueMarked = Marked<u8, Opaque>;

/// Serializes as nothing, whatever it holds.
pub struct Holder<B>(PhantomData<fn() -> B>);

impl<B> Serialize for Holder<B> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}

impl<'de, B> Deserialize<'de> for Holder<B> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <()>::deserialize(deserializer).map(|()| Self(PhantomData))
    }
}

/// A field whose own bound is empty: serde bounds nothing for it.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Held<B> {
    /// Its holder.
    #[serde(bound = "")]
    pub holder: Holder<B>,
}

/// A wire type, though what it holds is not one.
#[gdl_message::export]
pub type OpaqueHeld = Held<Opaque>;

/// A container bound that names one parameter: serde bounds that one
/// alone.
#[derive(Serialize, Deserialize, gdl_message::Message)]
#[serde(bound = "P: Serialize + serde::de::DeserializeOwned")]
pub struct Chosen<P, B> {
    /// What it carries.
    pub payload: P,
    /// Its holder.
    pub holder: Holder<B>,
}

/// A wire type: the bound names its payload, which is one.
#[gdl_message::export]
pub type OpaqueChosen = Chosen<u8, Opaque>;

/// Not one: the bound names its payload, which is not one.
#[gdl_message::export]
pub type ChosenOpaque = Chosen<Opaque, u8>;

/// A field's own bound holds under a container bound: serde adds each
/// field's bound before the container's.
#[derive(Serialize, Deserialize, gdl_message::Message)]
#[serde(bound = "")]
pub struct Pinned<T> {
    /// Which type, bounded as if it were held.
    #[serde(bound = "T: Serialize + serde::de::DeserializeOwned")]
    pub marker: PhantomData<T>,
}

/// A wire type: its argument is one.
#[gdl_message::export]
pub type BytePinned = Pinned<u8>;

/// Not one: the field's bound names its argument, which is not one.
#[gdl_message::export]
pub type OpaquePinned = Pinned<Opaque>;

/// A field's own bound holds though serde skips the field.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Skipped<T> {
    /// Which type, skipped and bounded.
    #[serde(skip, bound = "T: Serialize + serde::de::DeserializeOwned")]
    pub marker: PhantomData<T>,
}

/// A wire type: its argument is one.
#[gdl_message::export]
pub type ByteSkipped = Skipped<u8>;

/// Not one: the field's bound names its argument, which is not one.
#[gdl_message::export]
pub type OpaqueSkipped = Skipped<Opaque>;

/// A container bound in one direction: serde infers the other, where the
/// holder's field names `B`.
#[derive(Serialize, Deserialize, gdl_message::Message)]
#[serde(bound(deserialize = "P: serde::de::DeserializeOwned"))]
pub struct ChosenDe<P, B> {
    /// What it carries.
    pub payload: P,
    /// Its holder.
    pub holder: Holder<B>,
}

/// A wire type: both its arguments are.
#[gdl_message::export]
pub type ByteChosenDe = ChosenDe<u8, u8>;

/// Not one: serializing infers a bound on its holder's argument.
#[gdl_message::export]
pub type OpaqueChosenDe = ChosenDe<u8, Opaque>;

/// A field's own bound in one direction: serde infers the other.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct HeldDe<B> {
    /// Its holder.
    #[serde(bound(deserialize = ""))]
    pub holder: Holder<B>,
}

/// Not one: serializing infers a bound on what it holds.
#[gdl_message::export]
pub type OpaqueHeldDe = HeldDe<Opaque>;

/// A wire type that is not `Default`.
#[derive(Serialize, Deserialize)]
pub struct Bare(pub u8);

/// A wire type that implements `Default` by hand.
#[derive(Serialize, Deserialize)]
pub struct Manual(pub u8);

impl Default for Manual {
    fn default() -> Self {
        Self(1)
    }
}

/// A field serde fills with its default when it is missing: serde bounds
/// its parameter by `Default` too.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Defaulted<T> {
    /// The value.
    #[serde(default)]
    pub value: T,
}

/// Not one: its argument is not `Default`.
#[gdl_message::export]
pub type DefaultedBare = Defaulted<Bare>;

/// A wire type.
#[gdl_message::export]
pub type DefaultedByte = Defaulted<u8>;

/// A wire type: its argument implements `Default` by hand.
#[gdl_message::export]
pub type DefaultedManual = Defaulted<Manual>;

/// A field serde skips, and so fills with its default.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Hidden<T> {
    /// What it carries.
    pub value: u8,
    /// Never on the wire.
    #[serde(skip)]
    pub hidden: T,
}

/// Not one: its argument is not `Default`.
#[gdl_message::export]
pub type HiddenBare = Hidden<Bare>;

/// A wire type.
#[gdl_message::export]
pub type HiddenByte = Hidden<u8>;

/// A variant serde skips: it bounds nothing for its fields.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub enum Choice<T> {
    /// On the wire.
    Plain(u8),
    /// Never on the wire.
    #[serde(skip)]
    Other(T),
}

/// A wire type, though its argument is not one.
#[gdl_message::export]
pub type OpaqueChoice = Choice<Opaque>;

/// A variant with a bound of its own, in place of the inference for its
/// fields.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub enum Variant<T> {
    /// What it holds.
    #[serde(bound = "")]
    Held(Holder<T>),
}

/// A wire type, though its argument is not one.
#[gdl_message::export]
pub type OpaqueVariant = Variant<Opaque>;

/// Serde's traits written by hand for one instantiation whose argument is
/// not a wire type: serde's derive bounds nothing here.
#[derive(gdl_message::Message)]
pub struct Handmade<T>(pub T);

impl Serialize for Handmade<Opaque> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for Handmade<Opaque> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <()>::deserialize(deserializer).map(|()| Self(Opaque))
    }
}

/// A wire type, though its argument is not one.
#[gdl_message::export]
pub type OpaqueHandmade = Handmade<Opaque>;

/// Serde's traits written by hand for every instantiation, bounded as
/// written.
#[derive(gdl_message::Message)]
pub struct Wrapped<T>(pub T);

impl<T: Serialize> Serialize for Wrapped<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Wrapped<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self)
    }
}

/// A wire type: its argument is one.
#[gdl_message::export]
pub type ByteWrapped = Wrapped<u8>;

/// Not one: the impls bound its argument, which is not one.
#[gdl_message::export]
pub type OpaqueWrapped = Wrapped<Opaque>;

/// One direction derived, the other written by hand for every
/// instantiation without a bound: only the derived one bounds `T`.
#[derive(Serialize, gdl_message::Message)]
pub struct Half<T>(pub T);

impl<'de, T> Deserialize<'de> for Half<T> {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("never"))
    }
}

/// A wire type: its argument serializes.
#[gdl_message::export]
pub type ByteHalf = Half<u8>;

/// Not one: the derived direction bounds its argument.
#[gdl_message::export]
pub type OpaqueHalf = Half<Opaque>;

/// An enum whose derived `Default` is its unit variant: it bounds no
/// argument.
#[derive(Default)]
pub enum Maybe<T> {
    /// The default.
    #[default]
    Empty,
    /// A value.
    Value(T),
}

/// A struct whose derived `Default` bounds every argument.
#[derive(Default)]
pub struct Optional<T> {
    /// A value.
    pub value: Option<T>,
}

/// A wire type: `Maybe<Opaque>` is `Default`.
#[gdl_message::export]
pub type HiddenMaybe = Hidden<Maybe<Opaque>>;

/// Not one: `Optional<Opaque>` is not `Default`.
#[gdl_message::export]
pub type HiddenOptional = Hidden<Optional<Opaque>>;

/// A wire type that is not `Copy`, nor `Sync`.
#[derive(Serialize, Deserialize)]
pub struct Shared(pub std::cell::Cell<u8>);

/// Serde's traits by hand, bounded by `Copy` as well.
#[derive(gdl_message::Message)]
pub struct Copied<T>(pub T);

impl<T: Serialize + Copy> Serialize for Copied<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de> + Copy> Deserialize<'de> for Copied<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self)
    }
}

/// Not one: `Shared` is a wire type, but not `Copy`.
#[gdl_message::export]
pub type CopiedShared = Copied<Shared>;

/// Serde's traits by hand, bounded by `Sync` as well.
#[derive(gdl_message::Message)]
pub struct Synced<T>(pub T);

impl<T: Serialize + Sync> Serialize for Synced<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de> + Sync> Deserialize<'de> for Synced<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self)
    }
}

/// Not one: `Shared` is a wire type, but not `Sync`.
#[gdl_message::export]
pub type SyncedShared = Synced<Shared>;

/// Serde's traits by hand, `Deserialize` for one input lifetime alone:
/// borrowed data that lives forever.
#[derive(gdl_message::Message)]
pub struct Fixed<T>(pub T);

impl<T: Serialize> Serialize for Fixed<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<T: Deserialize<'static>> Deserialize<'static> for Fixed<T> {
    fn deserialize<D: Deserializer<'static>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer).map(Self)
    }
}

/// Not one: `Fixed<u8>` is not `DeserializeOwned`.
#[gdl_message::export]
pub type ByteFixed = Fixed<u8>;

/// Serde's traits by hand, for every input lifetime, bounded by its
/// argument's `Deserialize<'static>`: a wire type's argument has it.
#[derive(gdl_message::Message)]
pub struct Anchored<T>(pub T);

impl<T: Serialize> Serialize for Anchored<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'static>> Deserialize<'de> for Anchored<T> {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("never read"))
    }
}

/// A wire type: `u8` is `Deserialize<'static>`.
#[gdl_message::export]
pub type ByteAnchored = Anchored<u8>;

/// `Default` by hand, bounded by `Default`.
pub struct Fallback<T>(pub T);

impl<T: Default> Default for Fallback<T> {
    fn default() -> Self {
        Self(T::default())
    }
}

/// A wire type: `Fallback<u8>` is `Default`.
#[gdl_message::export]
pub type HiddenFallbackByte = Hidden<Fallback<u8>>;

/// Not one: `Fallback<Opaque>` is not `Default`.
#[gdl_message::export]
pub type HiddenFallbackOpaque = Hidden<Fallback<Opaque>>;

/// A container serde fills from its own default: serde bounds the type
/// itself by `Default`.
#[derive(Serialize, Deserialize, gdl_message::Message)]
#[serde(default)]
pub struct Whole<T> {
    /// Which type, as a type alone.
    pub marker: PhantomData<T>,
}

impl<T: Default> Default for Whole<T> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

/// A wire type: `Whole<u8>` is `Default`.
#[gdl_message::export]
pub type WholeByte = Whole<u8>;

/// Not one: `Whole<Opaque>` is not `Default`.
#[gdl_message::export]
pub type WholeOpaque = Whole<Opaque>;

/// A trait with an associated type.
pub trait Has {
    /// What it has.
    type Out;
}

impl Has for Opaque {
    type Out = Opaque;
}

/// A field of an associated type of its parameter: serde bounds
/// `T::Out`, not `T`.
#[derive(Serialize, Deserialize, gdl_message::Message)]
pub struct Projected<T: Has> {
    /// What its parameter has.
    pub out: T::Out,
}

/// Not one: `Opaque::Out` is `Opaque`.
#[gdl_message::export]
pub type ProjectedOpaque = Projected<Opaque>;

/// `Default`, though what it has is not: `Bare`.
#[derive(Default)]
pub struct Keyed;

impl Has for Keyed {
    type Out = Bare;
}

/// `derive(Default)` bounds a field's projection of a parameter,
/// `T::Out: Default`, as well as the parameter.
#[derive(Default)]
pub struct DefaultProjected<T: Has> {
    /// What its parameter has.
    pub out: T::Out,
}

/// Not one: `DefaultProjected<Keyed>` is not `Default`, since `Bare` is
/// not, though `Keyed` is.
#[gdl_message::export]
pub type HiddenDefaultProjected = Hidden<DefaultProjected<Keyed>>;

/// A type of another crate's, as serde's remote derive would mirror it.
pub struct Foreign {
    /// Its value.
    pub value: u8,
}

/// A remote definition: serde derives functions for `Foreign`, and no
/// trait for `Remote` itself.
#[derive(Serialize, Deserialize)]
#[serde(remote = "Foreign")]
pub struct Remote {
    /// Its value.
    pub value: u8,
}

/// Not one: `Remote` is not a wire type, though it derives serde's
/// traits.
#[gdl_message::export]
pub type OuterRemote = Outer<Remote>;

/// A type whose last field is unsized, and so is it.
pub struct Tail(pub [u8]);

/// A carrier of a type that may be unsized.
#[derive(gdl_message::Message)]
pub struct Boxed<T: ?Sized>(pub Box<T>);

/// Serde's traits by hand for every sized instantiation: an impl's
/// parameter is `Sized` unless it says `?Sized`.
impl<T> Serialize for Boxed<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}

impl<'de, T> Deserialize<'de> for Boxed<T> {
    fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("never"))
    }
}

/// A wire type: `u8` is sized.
#[gdl_message::export]
pub type ByteBoxed = Boxed<u8>;

/// Not one: `Tail` is unsized, and the impls are only for sized ones.
#[gdl_message::export]
pub type TailBoxed = Boxed<Tail>;

/// A module whose own trait `Default` hides the prelude's.
pub mod local_default {
    use serde::{Deserialize, Serialize};

    /// Not the standard library's.
    pub trait Default {}

    /// `Default` only by the local trait.
    pub struct Plain;

    impl Default for Plain {}

    /// A field serde skips, and so fills with the standard library's
    /// default.
    #[derive(Serialize, Deserialize, gdl_message::Message)]
    pub struct Skipping<T> {
        /// What it carries.
        pub value: u8,
        /// Never on the wire.
        #[serde(skip)]
        pub hidden: T,
    }

    /// Not one: `Plain` implements the local `Default`, not the standard
    /// library's.
    #[gdl_message::export]
    pub type LocalDefault = Skipping<Plain>;
}

/// A module whose own module `serde` hides the crate's.
pub mod local_serde {
    /// Not serde.
    mod serde {
        /// Not serde's.
        pub trait Serialize {}
        /// Not serde's.
        pub trait Deserialize<'de> {}
    }

    /// A carrier with the local traits alone.
    #[derive(gdl_message::Message)]
    pub struct Msg<T>(pub T);

    impl<T> serde::Serialize for Msg<T> {}

    impl<'de, T> serde::Deserialize<'de> for Msg<T> {}

    /// Not one: its traits are the local module's, not serde's.
    #[gdl_message::export]
    pub type LocalSerde = Msg<u8>;
}

/// Bounds by a trait of serde's name that is not serde's.
pub mod gated {
    use std::marker::PhantomData;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// A trait of serde's name, which `u8` does not implement.
    pub mod gate {
        /// Not serde's.
        pub trait Serialize {}
    }

    /// Serde's traits by hand, bounded by the other trait.
    #[derive(gdl_message::Message)]
    pub struct Gated<T>(pub PhantomData<T>);

    impl<T: gate::Serialize> Serialize for Gated<T> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_unit()
        }
    }

    impl<'de, T: gate::Serialize> Deserialize<'de> for Gated<T> {
        fn deserialize<D: Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
            Err(serde::de::Error::custom("never"))
        }
    }

    /// Not one: `u8` does not implement `gate::Serialize`.
    #[gdl_message::export]
    pub type GatedByte = Gated<u8>;

    /// A derive bounded, in place of serde's inference, by the other trait.
    #[derive(Serialize, Deserialize, gdl_message::Message)]
    #[serde(bound = "T: gate::Serialize")]
    pub struct GatedBound<T> {
        /// Which type, as a type alone.
        pub marker: PhantomData<T>,
    }

    /// Not one: `u8` does not implement `gate::Serialize`.
    #[gdl_message::export]
    pub type GatedBoundByte = GatedBound<u8>;
}

/// Instantiations whose arguments are the standard library's types.
pub mod standard {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use super::{Hidden, Opaque};

    /// A message body.
    #[derive(Serialize, Deserialize)]
    pub struct Body {
        /// The text.
        pub text: String,
    }

    /// A carrier of one value.
    #[derive(Serialize, Deserialize, gdl_message::Message)]
    pub struct Carry<T> {
        /// The value.
        pub value: T,
    }

    /// A carrier of two.
    #[derive(Serialize, Deserialize, gdl_message::Message)]
    pub struct Pair<A, B> {
        /// The first.
        pub a: A,
        /// The second.
        pub b: B,
    }

    /// A wire type.
    #[gdl_message::export]
    pub type CarryString = Carry<String>;

    /// A wire type.
    #[gdl_message::export]
    pub type CarryBodies = Carry<Vec<Body>>;

    /// A wire type.
    #[gdl_message::export]
    pub type BodyString = Pair<Body, String>;

    /// A wire type.
    #[gdl_message::export]
    pub type CarryMap = Carry<HashMap<String, Option<Body>>>;

    /// Not one: `Opaque` is not.
    #[gdl_message::export]
    pub type CarryOpaques = Carry<Vec<Opaque>>;

    /// Not one: `Opaque` is not.
    #[gdl_message::export]
    pub type CarryMaybeOpaque = Carry<Option<Opaque>>;

    /// A wire type: `Vec<Opaque>` is `Default`, whatever `Opaque` is.
    #[gdl_message::export]
    pub type HiddenOpaques = Hidden<Vec<Opaque>>;

    /// Not one: `Box<Opaque>` is `Default` only if `Opaque` is.
    #[gdl_message::export]
    pub type HiddenBoxed = Hidden<Box<Opaque>>;
}

/// Messages that are not generic, as real crates write them: the reader
/// judges them as it did before generic messages, and agrees with the
/// build.
pub mod plain {
    use serde::{Deserialize, Serialize};

    /// A validated newtype: serde through `try_from` and `into`.
    #[derive(Clone, Serialize, Deserialize, gdl_message::Message)]
    #[serde(try_from = "String", into = "String")]
    pub struct ModelName(String);

    impl TryFrom<String> for ModelName {
        type Error = String;

        fn try_from(name: String) -> Result<Self, String> {
            Ok(Self(name))
        }
    }

    impl From<ModelName> for String {
        fn from(name: ModelName) -> String {
            name.0
        }
    }

    /// A message in a child module that takes serde's derives from its
    /// parent's imports.
    pub mod sub {
        use super::*;

        /// A wire type.
        #[derive(Serialize, Deserialize, gdl_message::Message)]
        pub struct Globbed {
            /// A byte.
            pub x: u8,
        }
    }

    /// Serde's traits by hand, through `ser::` and `de::`.
    pub mod hand {
        use serde::{de, ser};

        /// A wire type.
        #[derive(gdl_message::Message)]
        pub struct Handed(pub u8);

        impl ser::Serialize for Handed {
            fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_u8(self.0)
            }
        }

        impl<'de> de::Deserialize<'de> for Handed {
            fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                <u8 as de::Deserialize>::deserialize(deserializer).map(Self)
            }
        }
    }

    /// A carrier of each.
    #[derive(Serialize, Deserialize, gdl_message::Message)]
    pub struct Holds<T>(pub T);

    /// A wire type: `ModelName` is one, through `String`.
    #[gdl_message::export]
    pub type HoldsName = Holds<ModelName>;
}
