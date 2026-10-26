import frappe

from frappe_s3_attachment.frappe_s3_attachment.doctype.s3_file_attachment_settings.s3_file_attachment_settings import (
    setup_custom_fields,
)


def execute():
    # the field only exists where S3 was enabled once; don't add it elsewhere
    if frappe.db.exists("Custom Field", {"dt": "File", "fieldname": "s3_file_key"}):
        setup_custom_fields()
