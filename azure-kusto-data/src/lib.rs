fn nothing() {
    let mut result = 3;
    let vec: Vec<isize> = Vec::new();
    if vec.len() <= 0 {}
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
