use gdl::Streamed;
use read_protocols as p;

/// A resampler: a stream with a head in, a stream without one out.
#[derive(gdl::Node)]
pub struct Resample;

impl gdl::Forward for Resample {
    type In = Streamed<p::Words, p::Token>;
    type Out = Streamed<(), p::Token>;
}

/// A detokenizer, to a message offered only through an export.
#[derive(gdl::Node)]
pub struct Detokenize;

impl gdl::Backward for Detokenize {
    type In = p::Token;
    type Out = p::Piece;
}
