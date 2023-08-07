macro_rules! match_unwrap {
    ($x: expr, $pat: pat, $res: expr) => {
        if let $pat = $x {
            $res
        } else {
            panic!("match unwrap failed!")
        }
    };
}
