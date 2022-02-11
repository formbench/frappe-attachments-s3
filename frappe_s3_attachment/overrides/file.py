import frappe
from frappe.utils import cint
from frappe.core.doctype.file.file import File

from frappe_s3_attachment.controller import S3Operations, is_s3_file_url


class S3File(File):
    @property
    def uploaded_to_s3(self):
        return bool(self.s3_file_key or is_s3_file_url(self.file_url or ""))

    def validate(self):
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

    def get_content(self):
        if not self.uploaded_to_s3:
            return super().get_content()

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

        s3 = S3Operations()
        s3.set_file_permission(self.s3_file_key, self.is_private)
        self.file_url = s3.get_file_url(
            self.s3_file_key, self.file_name, self.is_private
        )
