use gdl::Sink;

use super::{Token, Words};
use crate::sink::Engine;

impl Sink for Engine {
    type Req = Words;
    type Item = Token;
}
