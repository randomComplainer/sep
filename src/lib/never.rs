#[derive(Debug)]
pub enum Never {}

pub trait UnwrapNever {
    type Output;
    fn unwrap_never(self) -> Self::Output;
}

impl<T> UnwrapNever for Result<T, Never> {
    type Output = T;
    fn unwrap_never(self) -> Self::Output {
        match self {
            Ok(x) => x,
            Err(n) => match n {},
        }
    }
}
