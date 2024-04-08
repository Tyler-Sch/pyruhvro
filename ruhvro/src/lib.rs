#![warn(clippy::pedantic)]

pub mod deserialize;
mod schema_translate;
mod utils;

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        assert_eq!(4, 4);
    }
}
