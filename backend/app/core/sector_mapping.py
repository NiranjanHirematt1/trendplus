"""Sector normalization rules used across ingestion and compute jobs."""

SECTOR_NORMALIZATION_MAP = {
    "Agri - Animal Feed": "Agriculture", "Agri - Edible Oil": "Agriculture", "Agri - Misc": "Agriculture",
    "Agri - Rice": "Agriculture", "Agri - Sugar": "Agriculture", "Agri - Tea/Coffee": "Agriculture",
    "Agrichem - Fertilizers": "Agriculture", "Agrichem - Protection": "Agriculture",
    "Auto - 2W": "Auto & Ancillaries", "Auto - CV": "Auto & Ancillaries", "Auto - Misc": "Auto & Ancillaries",
    "Auto - OEM Suppliers": "Auto & Ancillaries", "Auto - PV/Trucks": "Auto & Ancillaries",
    "Auto - Replacement": "Auto & Ancillaries", "Auto - Tyres": "Auto & Ancillaries",
    "Automobile and Auto Components": "Auto & Ancillaries",
    "Financials - Private Bank": "Banking", "Financials - PSU Bank": "Banking", "Financials - Housing": "Banking",
    "Engineering": "Capital Goods", "Electronic - Equipment": "Capital Goods", "Industrial - Bearings": "Capital Goods",
    "Industrial - Consumables": "Capital Goods", "Industrial - Pumps/Engines": "Capital Goods",
    "Chemicals - Bulk": "Chemicals", "Chemicals - Misc": "Chemicals", "Chemicals - Petro": "Chemicals", "Chemicals - Specialty": "Chemicals",
    "Construction Materials": "Construction", "Infra - Cement": "Construction", "Infra - Ceramics": "Construction",
    "Infra - Construction": "Construction", "Infra - Diversified": "Construction", "Infra - Granites/Marbles": "Construction", "Infra - Plastic Pipes": "Construction",
    "Consumption - Appliances": "Consumer Durables", "Consumption - Electronics": "Consumer Durables", "Consumption - Jewelry": "Consumer Durables",
    "Consumption - Misc": "Consumer Durables", "Consumption - Paints": "Consumer Durables", "Consumption - Retail": "Consumer Durables",
    "Leisure - Hotels": "Consumer Services", "Leisure - Misc": "Consumer Services", "Human Resources": "Consumer Services", "Services": "Consumer Services",
    "Defense - Misc": "Defence", "Defense - Ship Building": "Defence", "Defense - Technology": "Defence",
    "Others": "Diversified",
    "Fast Moving Consumer Goods": "FMCG", "Beverages - Alcohol": "FMCG", "Consumption - FMCG": "FMCG", "Consumption - Dairy": "FMCG", "Consumption - Personal": "FMCG", "Forest Materials": "FMCG",
    "Financials - AMC": "Financial Services", "Financials - Broking": "Financial Services", "Financials - Life": "Financial Services", "Financials - MFI": "Financial Services", "Financials - Misc": "Financial Services", "Financials - NBFC": "Financial Services", "Financials - Non-Life": "Financial Services", "Financials - Ratings": "Financial Services",
    "Healthcare": "Healthcare & Pharma", "Healthcare - Biotech": "Healthcare & Pharma", "Healthcare - Hospitals": "Healthcare & Pharma", "Healthcare - Pharma": "Healthcare & Pharma",
    "IT - Equipments": "Information Technology", "IT - Product/Platform": "Information Technology", "IT - Services": "Information Technology", "IT - Software/Consulting": "Information Technology",
    "Media - Films": "Media & Entertainment", "Media - Publishing": "Media & Entertainment", "Media - TV Broadcast": "Media & Entertainment", "Media Entertainment & Publication": "Media & Entertainment",
    "Metals - Aluminium": "Metals & Mining", "Metals - Casting/Forging": "Metals & Mining", "Metals - Copper": "Metals & Mining", "Metals - Iron & Steel": "Metals & Mining", "Metals - Misc": "Metals & Mining", "Metals - Pipe & Tube": "Metals & Mining", "Mining - Coal": "Metals & Mining",
    "Oil and Gas": "Oil, Gas & Energy", "Oil Gas & Consumable Fuels": "Oil, Gas & Energy", "Petro - Refineries": "Oil, Gas & Energy",
    "Paper": "Paper & Packaging", "Packaging": "Paper & Packaging",
    "Power - Equipment": "Power", "Power - Misc": "Power", "Power - Wiring": "Power", "Utilities": "Power", "Utility - Gas": "Power", "Utility - Misc": "Power",
    "Realty": "Real Estate", "Telecommunication": "Telecom",
    "Textile - Apparels": "Textiles", "Textile - Misc": "Textiles", "Textile - Spinning/Yarn": "Textiles", "Textiles - Polyester": "Textiles", "Leather Products": "Textiles",
    "Transport - Logistics": "Transport & Logistics", "Transport - Shipping": "Transport & Logistics",
}

def normalize_sector_name(sector: str) -> str:
    sec = str(sector or "").strip()
    return SECTOR_NORMALIZATION_MAP.get(sec, sec)
