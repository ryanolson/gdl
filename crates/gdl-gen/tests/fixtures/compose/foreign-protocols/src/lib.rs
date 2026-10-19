//! A protocols crate built against another gdl-message: it registers into that
//! gdl's catalogue, so the program links both.

#[gdl_message::linkme::distributed_slice(gdl_message::GDL_MESSAGE_CATALOGUE)]
#[linkme(crate = gdl_message::linkme)]
static FOREIGN: &str = "foreign";
