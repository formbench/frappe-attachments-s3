import frappe

from frappe_s3_attachment.controller import (
    S3Operations,
    get_files_with_key,
    get_key_from_public_url,
)


def execute():
    # copies of public Files got no key, so deleting or hiding the original broke them
    if not frappe.db.get_single_value(
        "S3 File Attachment Settings", "enable_s3_file_attachment"
    ):
        return

    s3 = S3Operations()
    files = frappe.get_all(
        "File",
        filters={"s3_file_key": ("is", "not set"), "file_url": ("like", "https://%")},
        fields=["name", "file_url"],
    )
    for file in files:
        key = get_key_from_public_url(file.file_url, s3)
        # a URL from before the object was made private must not make it public again
        if key and not get_files_with_key(key, {"is_private": 1}):
            frappe.db.set_value(
                "File", file.name, "s3_file_key", key, update_modified=False
            )
