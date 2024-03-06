#![warn(clippy::pedantic)]

pub mod deserialize;
mod utils;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(4, 4);
    }
}
