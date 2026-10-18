//! A protocols crate built against another gdl: it registers into that
//! gdl's catalogue, so the program links both.

#[gdl::linkme::distributed_slice(gdl::GDL_MESSAGE_CATALOGUE)]
#[linkme(crate = gdl::linkme)]
static FOREIGN: &str = "foreign";
