from entsoe.mappings import (
    BSNTYPE,
    DOCSTATUS,
    DOCUMENTTYPE,
    MARKETAGREEMENTTYPE,
    NEIGHBOURS,
    PROCESSTYPE,
    PSRTYPE_MAPPINGS,
    Area,
)


def _invert_dict(d: dict) -> dict:
    return {v: k for k, v in d.items()}


class EntsoeMappings:
    """
    Centralized, bi-directional SSOT access to all ENTSO-E mappings.
    """

    # Code -> Name (Forward Lookups) ---
    psr_code_to_name = PSRTYPE_MAPPINGS
    bsn_code_to_name = BSNTYPE
    process_code_to_name = PROCESSTYPE
    doc_code_to_name = DOCUMENTTYPE
    docstatus_code_to_name = DOCSTATUS
    market_agreement_code_to_name = MARKETAGREEMENTTYPE

    # Name -> Code (Reverse Lookups) ---
    psr_name_to_code = _invert_dict(PSRTYPE_MAPPINGS)
    bsn_name_to_code = _invert_dict(BSNTYPE)
    process_name_to_code = _invert_dict(PROCESSTYPE)
    doc_name_to_code = _invert_dict(DOCUMENTTYPE)
    docstatus_name_to_code = _invert_dict(DOCSTATUS)
    market_agreement_name_to_code = _invert_dict(MARKETAGREEMENTTYPE)

    # Graph Data
    neighbours = NEIGHBOURS

    @classmethod
    def get_bidding_zones(cls) -> list[dict]:
        """Extracts structured bidding zone data from Area enum"""
        return [
            {
                "area_short_code": area.name,
                "country_code": area.value,
                "meaning": area.meaning,
                "timezone": area.tz,
            }
            for area in Area
        ]
