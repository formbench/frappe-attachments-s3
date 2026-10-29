import frappe
from frappe.utils import cint
from frappe.core.doctype.file.file import File

from frappe_s3_attachment.controller import (
    S3Operations,
    check_key_access,
    get_files_with_key,
    is_s3_file_url,
)


class S3File(File):
    @property
    def uploaded_to_s3(self):
        return bool(self.get('s3_file_key') or is_s3_file_url(self.file_url or ""))

    def validate(self):
        # s3_file_key and file_url are read-only in the UI only; REST clients can set them
        if self.get("s3_file_key") and (
            self.has_value_changed("s3_file_key") or self.has_value_changed("file_url")
        ):
            # core strips markup after validate, which would store a key never checked
            if "<" in self.s3_file_key or ">" in self.s3_file_key:
                frappe.throw(frappe._("Invalid S3 key"))

            check_key_access(self.s3_file_key, self)

        if not self.uploaded_to_s3:
            return super().validate()

        self.handle_is_private_changed()

    def generate_content_hash(self):
        if not self.uploaded_to_s3:
            return super().generate_content_hash()

    def validate_url(self):
        if not self.uploaded_to_s3:
            return super().validate_url()

    def set_is_private(self):
        if not self.uploaded_to_s3:
            return super().set_is_private()

        if not self.is_private:
            self.is_private = cint(self.file_url.startswith("/api/method"))

    # v16 added `encodings`; v15 takes no arguments
    def get_content(self, *args, **kwargs):
        if not self.uploaded_to_s3:
            return super().get_content(*args, **kwargs)

        s3 = S3Operations()
        try:
            res_body = s3.read_file(self.s3_file_key).get("Body")
            if res_body:
                return res_body.read()

        except Exception as e:
            frappe.throw(f"Error while reading file from s3: {e}")

    def handle_is_private_changed(self):
        if not self.uploaded_to_s3:
            return super().handle_is_private_changed()

        if not self.get_doc_before_save() or not self.has_value_changed("is_private"):
            return

        # copies share the object, so its ACL and their URLs change with this File
        copies = [
            name for name in get_files_with_key(self.s3_file_key) if name != self.name
        ]
        if not all(frappe.has_permission("File", "write", doc=name) for name in copies):
            frappe.throw(frappe._("Not permitted"), frappe.PermissionError)

        s3 = S3Operations()
        s3.set_file_permission(self.s3_file_key, self.is_private)
        old_file_url = self.file_url
        self.file_url = s3.get_file_url(
            self.s3_file_key, self.file_name, self.is_private
        )

        update_existing_file_docs(self, copies)

        if (
            not self.attached_to_doctype
            or not self.attached_to_name
            or not self.fetch_attached_to_field(old_file_url)
        ):
            return

        frappe.db.set_value(
            self.attached_to_doctype,
            self.attached_to_name,
            self.attached_to_field,
            self.file_url,
        )


def update_existing_file_docs(doc, copies):
    if not copies:
        return

    # Update is private and file url of all file docs that point to the same file
    frappe.db.sql(
        """
        UPDATE `tabFile`
        SET
            file_url = %(file_url)s,
            is_private = %(is_private)s
        WHERE
            name in %(copies)s
    """,
        dict(
            file_url=doc.file_url,
            is_private=doc.is_private,
            copies=copies,
        ),
    )
