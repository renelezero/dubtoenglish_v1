"""
Vessel identification from MMSI and public databases.

MMSI Maritime Identification Digits (MID) decode country of registration.
Also provides vessel type enrichment from known vessel databases.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# MMSI MID → Country mapping (first 3 digits of 9-digit MMSI)
# Source: ITU Maritime Identification Digits
MID_COUNTRY = {
    201: "AL", 202: "AD", 203: "AT", 204: "PT", 205: "BE", 206: "BY",
    207: "BG", 209: "CY", 210: "CY", 211: "DE", 212: "CY", 213: "GE",
    214: "MD", 215: "MT", 216: "AM", 218: "DE", 219: "DK", 220: "DK",
    224: "ES", 225: "ES", 226: "FR", 227: "FR", 228: "FR", 229: "MT",
    230: "FI", 231: "FO", 232: "GB", 233: "GB", 234: "GB", 235: "GB",
    236: "GI", 237: "GR", 238: "HR", 239: "GR", 240: "GR", 241: "GR",
    242: "MA", 243: "HU", 244: "NL", 245: "NL", 246: "NL", 247: "IT",
    248: "MT", 249: "MT", 250: "IE", 251: "IS", 252: "LI", 253: "LU",
    254: "MC", 255: "PT", 256: "MT", 257: "NO", 258: "NO", 259: "NO",
    261: "PL", 263: "PT", 264: "RO", 265: "SE", 266: "SE", 267: "SK",
    268: "SM", 269: "CH", 270: "CZ", 271: "TR", 272: "UA", 273: "RU",
    274: "MK", 275: "LV", 276: "EE", 277: "LT", 278: "SI", 279: "ME",
    301: "AI", 303: "US", 304: "AG", 305: "AG", 306: "CW", 307: "AW",
    308: "BS", 309: "BS", 310: "BM", 311: "BS", 312: "BZ", 314: "BB",
    316: "CA", 319: "KY", 321: "CR", 323: "CU", 325: "DM", 327: "DO",
    329: "GP", 330: "GD", 331: "GL", 332: "GT", 334: "HN", 336: "HT",
    338: "US", 339: "JM", 341: "KN", 343: "LC", 345: "MX", 347: "MQ",
    348: "MS", 350: "NI", 351: "PA", 352: "PA", 353: "PA", 354: "PA",
    355: "PA", 356: "PA", 357: "PA", 358: "PR", 359: "SV", 361: "PM",
    362: "TT", 364: "TC", 366: "US", 367: "US", 368: "US", 369: "US",
    370: "PA", 371: "PA", 372: "PA", 373: "PA", 374: "PA", 375: "VC",
    376: "VC", 377: "VC", 378: "VG", 379: "VI",
    401: "AF", 403: "SA", 405: "BD", 408: "BH", 410: "BT", 412: "CN",
    413: "CN", 414: "CN", 416: "TW", 417: "LK", 419: "IN", 422: "IR",
    423: "AZ", 425: "IQ", 428: "IL", 431: "JP", 432: "JP", 434: "TM",
    436: "KZ", 437: "UZ", 438: "JO", 440: "KR", 441: "KR", 443: "PS",
    445: "KP", 447: "KW", 450: "LB", 451: "KG", 453: "MO", 455: "MV",
    457: "MN", 459: "NP", 461: "OM", 463: "PK", 466: "QA", 468: "SY",
    470: "AE", 471: "AE", 472: "TJ", 473: "YE", 475: "SA",
    477: "HK", 478: "BA",
    501: "AQ", 503: "AU", 506: "MM", 508: "BN", 510: "FM", 511: "PW",
    512: "NZ", 514: "KH", 515: "KH", 516: "CX", 518: "CK", 520: "FJ",
    523: "CC", 525: "ID", 529: "KI", 531: "LA", 533: "MY", 536: "MP",
    538: "MH", 540: "NC", 542: "NU", 544: "NR", 546: "PF", 548: "PH",
    553: "PG", 555: "PN", 557: "SB", 559: "AS", 561: "WS", 563: "SG",
    564: "SG", 565: "SG", 566: "SG", 567: "TH", 570: "TO", 572: "TV",
    574: "VN", 576: "VU", 577: "VU", 578: "WF",
    601: "ZA", 603: "AO", 605: "DZ", 607: "TF", 608: "IO", 609: "BI",
    610: "BJ", 611: "BW", 612: "CF", 613: "CM", 615: "CG", 616: "KM",
    617: "CV", 618: "AQ", 619: "CI", 620: "DJ", 621: "EG", 622: "ET",
    624: "ER", 625: "GA", 626: "GH", 627: "GM", 629: "GN", 630: "GQ",
    631: "GW", 632: "GN", 633: "KE", 634: "LR", 635: "LR", 636: "LR",
    637: "LR", 638: "SS", 642: "LY", 644: "LS", 645: "MU", 647: "MG",
    649: "ML", 650: "MZ", 654: "MR", 655: "MW", 656: "NE", 657: "NG",
    659: "NA", 660: "RE", 661: "RW", 662: "ST", 663: "SN", 664: "SC",
    665: "SL", 666: "SO", 667: "SZ", 668: "SD", 669: "TZ", 670: "TD",
    671: "TG", 672: "TN", 674: "UG", 675: "CD", 676: "TZ", 677: "TZ",
    678: "ZM", 679: "ZW",
}

COUNTRY_NAMES = {
    "AE": "UAE", "SA": "Saudi Arabia", "IR": "Iran", "IQ": "Iraq",
    "KW": "Kuwait", "BH": "Bahrain", "QA": "Qatar", "OM": "Oman",
    "US": "USA", "GB": "UK", "CN": "China", "IN": "India",
    "PA": "Panama", "LR": "Liberia", "MH": "Marshall Islands",
    "MT": "Malta", "HK": "Hong Kong", "SG": "Singapore",
    "BS": "Bahamas", "CY": "Cyprus", "GR": "Greece", "NO": "Norway",
    "DE": "Germany", "NL": "Netherlands", "DK": "Denmark", "JP": "Japan",
    "KR": "South Korea", "TR": "Turkey", "FR": "France", "IT": "Italy",
    "ES": "Spain", "PT": "Portugal", "SE": "Sweden", "FI": "Finland",
    "PH": "Philippines", "ID": "Indonesia", "MY": "Malaysia",
    "TH": "Thailand", "VN": "Vietnam", "BD": "Bangladesh",
    "PK": "Pakistan", "LK": "Sri Lanka", "MM": "Myanmar",
    "EG": "Egypt", "JO": "Jordan", "LB": "Lebanon", "SY": "Syria",
    "YE": "Yemen", "IL": "Israel", "GE": "Georgia", "AZ": "Azerbaijan",
    "RU": "Russia", "UA": "Ukraine", "KP": "North Korea",
    "VE": "Venezuela", "CU": "Cuba", "BR": "Brazil", "MX": "Mexico",
    "AU": "Australia", "NZ": "New Zealand", "ZA": "South Africa",
    "NG": "Nigeria", "KE": "Kenya", "TZ": "Tanzania",
    "GI": "Gibraltar", "VC": "St Vincent", "VG": "British Virgin Is",
    "HR": "Croatia", "DM": "Dominica", "AG": "Antigua", "TW": "Taiwan",
}

# Flags of convenience — commonly used to obscure true ownership
FLAGS_OF_CONVENIENCE = {"PA", "LR", "MH", "BS", "MT", "CY", "HK", "SG", "VU", "KN", "VC", "DM", "AG"}

# Sanctioned country flags worth flagging
SANCTIONED_FLAGS = {"IR", "KP", "SY", "CU", "VE", "RU"}


def mmsi_to_country_code(mmsi: int) -> str:
    """Decode the first 3 digits of MMSI to ISO country code."""
    mid = mmsi // 1_000_000
    return MID_COUNTRY.get(mid, "")


def mmsi_to_country_name(mmsi: int) -> str:
    code = mmsi_to_country_code(mmsi)
    return COUNTRY_NAMES.get(code, code)


def is_flag_of_convenience(mmsi: int) -> bool:
    return mmsi_to_country_code(mmsi) in FLAGS_OF_CONVENIENCE


def is_sanctioned_flag(mmsi: int) -> bool:
    return mmsi_to_country_code(mmsi) in SANCTIONED_FLAGS


def enrich_vessel(vessel: dict) -> dict:
    """Add country info and flag analysis to a vessel dict."""
    mmsi = vessel.get("mmsi", 0)
    country_code = mmsi_to_country_code(mmsi)
    country_name = mmsi_to_country_name(mmsi)

    vessel["flag_country_code"] = country_code
    vessel["flag_country"] = country_name
    vessel["flag_of_convenience"] = country_code in FLAGS_OF_CONVENIENCE
    vessel["sanctioned_flag"] = country_code in SANCTIONED_FLAGS

    if not vessel.get("flag"):
        vessel["flag"] = country_code

    return vessel


# Known vessel name patterns for type inference when static data is missing
_TANKER_PATTERNS = [
    "VLCC", "ULCC", "SUEZMAX", "AFRAMAX", "PANAMAX",
    "CRUDE", "OIL", "PETROL", "CHEMICAL", "LNG", "LPG",
    "TANKER", "SPIRIT", "STAR", "FRONT",
]

_MILITARY_PATTERNS = [
    "USS ", "HMS ", "HMCS ", "HMAS ", "INS ",
    "KD ", "IRIN ", "P-", "WARSHIP",
]

_CARGO_PATTERNS = [
    "EXPRESS", "CARRIER", "TRADER", "TRANSPORT",
    "BULK", "GENERAL", "FEEDER", "BOX",
]


def infer_vessel_type(name: str, current_type: str) -> str:
    """Try to guess vessel type from name if static data hasn't arrived."""
    if current_type and current_type != "other":
        return current_type

    upper = (name or "").upper()
    if not upper:
        return current_type

    for pattern in _MILITARY_PATTERNS:
        if pattern in upper:
            return "military"
    for pattern in _TANKER_PATTERNS:
        if pattern in upper:
            return "tanker"
    for pattern in _CARGO_PATTERNS:
        if pattern in upper:
            return "cargo"
    if "TUG" in upper or "PILOT" in upper:
        return "special"

    return current_type
