use gdl::Streamed;
use read_protocols as p;

/// A resampler: a stream with a head in, a stream without one out.
pub struct Resample;

#[gdl::node]
impl gdl::Forward for Resample {
    type In = Streamed<p::Words, p::Token>;
    type Out = Streamed<(), p::Token>;
}

/// A detokenizer, to a message offered only through an export.
pub struct Detokenize;

#[gdl::node]
impl gdl::Backward for Detokenize {
    type In = p::Token;
    type Out = p::Piece;
}
