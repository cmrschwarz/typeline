#[macro_export]
macro_rules! count {
    (
        $_a:tt $_b:tt $_c:tt $_d:tt $_e:tt $_f:tt $_g:tt $_h:tt $_i:tt $_j:tt
        $_k:tt $_l:tt $_m:tt $_n:tt $_o:tt $_p:tt $_q:tt $_r:tt $_s:tt $_t:tt
        $($tail:tt)*
    ) => {
        20usize + $crate::count!($($tail)*)
    };
    (
        $_a:tt $_b:tt $_c:tt $_d:tt $_e:tt $_f:tt $_g:tt $_h:tt $_i:tt $_j:tt
        $($tail:tt)*
    ) => {
        10usize + $crate::count!($($tail)*)
    };
    (
        $_a:tt $_b:tt $_c:tt $_d:tt $_e:tt $($tail:tt)*
    ) => {
        5usize + $crate::count!($($tail)*)
    };
    ($_a:tt $($tail:tt)*) => {
        1usize + $crate::count!($($tail)*)
    };
    () => {
        0usize
    };
}

#[macro_export]
macro_rules! typelist {
    {$(
        $visibility: vis mod $name: ident $(: ($($base_traits: tt)+))? = [ $first: path $(, $($types: path),*)? ] {
            $($module_body: tt)*
        }
    )*} => {
        $(

            $visibility mod $name {
                #[allow(unused, clippy::wildcard_imports)]
                use super::*;

                $($module_body)*

                #[allow(unused)]
                pub trait TypeList {
                    type Value: TypeList $(+ $($base_traits)+ )?;
                    type Next: TypeList;
                    #[allow(unused)]
                    const INDEX: usize;
                    #[inline(always)]
                    fn foreach<A: Apply>(applicator: &mut A) {
                        applicator.apply::<<Self as TypeList>::Value>();
                        if Self::INDEX + 1 != COUNT {
                            Self::Next::foreach(applicator);
                        }
                    }
                }

                #[allow(unused)]
                pub trait Apply {
                    fn apply<T: TypeList $(+ $($base_traits)+ )?>(&mut self);
                }

                #[allow(unused)]
                pub fn foreach<A: Apply>(applicator: &mut A) {
                    <$first as TypeList>::foreach(applicator)
                }

                #[allow(unused)]
                pub const COUNT: usize = $crate::count!([$first] $( $([$types])* )?);

                $crate::typelist!(@impls ($crate::count!([$first] $( $([$types])* )?)), $first $(, $($types),*)?);
            }
        )*
    };
    (@impls ($count_total: expr), $first: path, $second: path $(, $($rest: path),*)?) => {
        impl TypeList for $first {
            type Value = $first;
            type Next = $second;
            const INDEX: usize = $count_total - $crate::count!( [$first] [$second] $( $([$rest])* )?);
        }
        $crate::typelist!(@impls ($count_total), $second $(, $($rest),* )?);
    };
    (@impls ($count_total: expr), $first: path $(,)?) => {
        impl TypeList for $first {
            type Value = $first;
            type Next = $first;
            const INDEX: usize = $count_total - 1;
        }
    };
    (@impls first: ,?) => {};
}

#[cfg(test)]
mod test {

    use num::Bounded;

    typelist! {
        mod unsized_list: (Bounded + Into<u64>) = [u8, u16, u32, u64] {}

        mod random_list = [Foo, f32, Bar, i64] {
            pub struct Foo;
            pub struct Bar;
        }
    }

    #[test]
    fn indices() {
        struct Index(Vec<usize>);
        impl random_list::Apply for Index {
            fn apply<T: random_list::TypeList>(&mut self) {
                self.0.push(T::INDEX)
            }
        }
        let mut index = Index(Vec::new());
        random_list::foreach(&mut index);

        assert_eq!(&index.0, &[0, 1, 2, 3]);
        assert_eq!(random_list::COUNT, 4);
    }

    #[test]
    fn multi_trait_test() {
        struct Bits(Vec<u32>);
        impl unsized_list::Apply for Bits {
            fn apply<T: Bounded + Into<u64>>(&mut self) {
                self.0
                    .push(<T as Into<u64>>::into(T::max_value()).count_ones())
            }
        }
        let mut bits = Bits(Vec::new());
        unsized_list::foreach(&mut bits);
        assert_eq!(&bits.0, &[i8::BITS, i16::BITS, i32::BITS, i64::BITS]);
    }
}
