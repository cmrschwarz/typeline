#[derive(Clone, Copy)]
pub struct HwInfo {
    #[cfg(all(
        target_arch = "x86_64",
        not(target_feature = "avx2"),
        not(feature = "debug_disable_avx2"),
        not(feature = "debug_force_avx2")
    ))]
    supports_avx2: bool,
}

impl Default for HwInfo {
    fn default() -> Self {
        Self {
            #[cfg(all(
                target_arch = "x86_64",
                not(target_feature = "avx2"),
                not(feature = "debug_disable_avx2"),
                not(feature = "debug_force_avx2")
            ))]
            supports_avx2: std::is_x86_feature_detected!("avx2"),
        }
    }
}

impl HwInfo {
    #[inline(always)]
    pub fn avx2(self) -> bool {
        cfg_if::cfg_if! {
            if #[cfg(not(target_arch = "x86_64"))] {
                false
            }
            else if #[cfg(feature = "debug_force_avx2")] {
                true
            }
            else if #[cfg(feature = "debug_disable_avx2")] {
                false
            }
            else if #[cfg(target_feature = "avx2")] {
                true
            } else {
                self.supports_avx2
            }
        }
    }
}
