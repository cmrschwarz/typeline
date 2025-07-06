#[derive(Clone, Copy)]
pub struct HwInfo {
    #[cfg(all(
        target_arch = "x86_64",
        not(target_feature = "avx2"),
        not(feature = "debug_disable_avx2")
    ))]
    supports_avx2: bool,
}

impl Default for HwInfo {
    fn default() -> Self {
        Self {
            #[cfg(all(
                target_arch = "x86_64",
                not(target_feature = "avx2"),
                not(feature = "debug_disable_avx2")
            ))]
            supports_avx2: std::is_x86_feature_detected!("avx2"),
        }
    }
}

impl HwInfo {
    #[inline(always)]
    pub fn avx2(self) -> bool {
        cfg_if::cfg_if! {
            if #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2"), not(feature="debug_disable_avx2")))] {
                self.supports_avx2
            } else {
                cfg!(all(target_feature = "avx2", not(feature="debug_disable_avx2")))
            }
        }
    }
}
