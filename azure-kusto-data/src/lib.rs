fn nothing() -> u32 {
    return 42;
}

#[cfg(test)]
mod tests {
    use crate::nothing;

    #[test]
    fn it_works() {
        let result = 2 + nothing();
        assert_eq!(result, 44);
    }
}
