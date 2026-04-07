pub struct AttributionSignals {
    pub exact_type2_npi_match: bool,
    pub address_proximity_match: bool,
    pub taxonomy_compatible: bool,
    pub payer_plan_consistent: bool,
}

pub const DEFAULT_CONFIDENCE_THRESHOLD: f64 = 0.90;

pub fn score(signals: &AttributionSignals) -> f64 {
    let mut total: f64 = 0.0;

    if signals.exact_type2_npi_match {
        total += 0.55;
    }
    if signals.address_proximity_match {
        total += 0.20;
    }
    if signals.taxonomy_compatible {
        total += 0.15;
    }
    if signals.payer_plan_consistent {
        total += 0.10;
    }

    (total * 100.0).round() / 100.0
}

pub fn is_displayable(confidence: f64) -> bool {
    confidence >= DEFAULT_CONFIDENCE_THRESHOLD
}
