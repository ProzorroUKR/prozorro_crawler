from settings import STORE_CLAIMS, STORE_DRAFTS, STORE_WO_DATE
from settings import TENDER_FIELDS, LOT_FIELDS


def filter_keys(data, keys):
    return {k: v for k, v in data.items() if k in keys}


def get_all_complaints(data):
    for c in data.get("complaints", ""):
        yield c

    for award in data.get("awards", ""):
        related_lot = award.get("lotID")
        for c in award.get("complaints", ""):
            if related_lot:
                c["relatedLot"] = related_lot
            yield c

    for qualification in data.get("qualifications", ""):
        related_lot = qualification.get("lotID")
        for c in qualification.get("complaints", ""):
            if related_lot:
                c["relatedLot"] = related_lot
            yield c


def filter_out(complaints):
    # July 2, 2016 by Julia Dvornyk, don't store complaint.type == 'claim'
    # July 26, 2016 by Andriy Kucherenko, don't store complaint.status == 'draft'
    # Aug 11, 2016 by Julia Dvornyk, don't store w/o dateSubmitted
    for complaint in complaints:
        if (
            complaint.get("type") == "claim" and not STORE_CLAIMS
            or complaint.get("status") == "draft" and not STORE_DRAFTS
            or not complaint.get("dateSubmitted") and not STORE_WO_DATE
        ):
            continue

        yield complaint


def get_lot(data, lot_id):
    for lot in data.get("lots", ""):
        if lot.get("id") == lot_id:
            return filter_keys(lot, LOT_FIELDS)


def get_complaint_data(data):
    tender_data = filter_keys(data, TENDER_FIELDS)
    complaints = get_all_complaints(data)
    for complaint in filter_out(complaints):
        complaint_data = dict(
            tender=tender_data,
            complaint=complaint,
        )
        related_lot = complaint.get("relatedLot")
        if related_lot:
            complaint_data["lot"] = get_lot(data, related_lot)

        complaint_data["cancellations"] = [
            cancellation
            for cancellation in data.get("cancellations", "")
            if cancellation.get("relatedLot") in (None, related_lot)
        ]
        yield complaint_data

