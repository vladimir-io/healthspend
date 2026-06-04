"""In-memory hospital index for fast cms-hpt location → CCN matching."""

from __future__ import annotations

from cms_hpt_utils import (
    CmsHptLocation,
    HospitalRow,
    extract_ccn_from_text,
    extract_state_hint,
    match_ccn,
    normalize_name,
)


class HospitalMatchIndex:
    """State-partitioned index: O(candidates in state) instead of O(all hospitals)."""

    def __init__(self, hospitals: list[HospitalRow]) -> None:
        self.by_ccn: dict[str, HospitalRow] = {h.ccn: h for h in hospitals}
        self.by_state: dict[str, list[HospitalRow]] = {}
        for h in hospitals:
            self.by_state.setdefault(h.state, []).append(h)

    @classmethod
    def from_csv_rows(cls, hospitals: list[HospitalRow]) -> HospitalMatchIndex:
        return cls(hospitals)

    def candidates_for(self, loc: CmsHptLocation) -> list[HospitalRow]:
        state = extract_state_hint(loc.location_name) or ""
        if state and state in self.by_state:
            return self.by_state[state]
        return list(self.by_ccn.values())

    def resolve(
        self, loc: CmsHptLocation, min_score: float = 0.80
    ) -> tuple[str, float] | None:
        for hint in (loc.ccn_hint, extract_ccn_from_text(loc.location_name)):
            if hint and hint in self.by_ccn:
                return hint, 0.99

        if not loc.location_name:
            return None

        pool = self.candidates_for(loc)
        if not pool:
            return None

        state_hint = extract_state_hint(loc.location_name)
        if state_hint:
            return match_ccn(loc.location_name, pool, min_score)

        # No state in location line — still match but require higher confidence.
        return match_ccn(loc.location_name, pool, max(min_score, 0.86))
