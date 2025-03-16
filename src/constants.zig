pub const PAGE_SIZE: u16 = 4096;
pub const CACHE_LINE_SIZE: u8 = 64;

pub const ApplicationError = error{
    ExecutionTimeout,
    InternalError,
};
