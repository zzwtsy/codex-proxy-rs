use icu_casemap::CaseMapper;
use unicode_normalization::UnicodeNormalization;

pub(super) fn normalize_name_key(name: &str) -> String {
    let canonical_name = name.nfc().collect::<String>();
    let folded_name = CaseMapper::new().fold_string(&canonical_name);
    folded_name.as_ref().nfc().collect()
}
