from frappe.core.doctype.file.file import File

from frappe_s3_attachment.controller import S3Operations


class S3File(File):
    # @property
    # def file_url(self):
    # 	if not self.get("file_url", "").startswith("/api/method"):
    # 		return self.get("file_url")

    # 	return get_url(self.get("file_url"))

    # def is_remote_file(self) -> bool:
    #     return self.file_url.startswith(("http", "/api/method"))

    @property
    def uploaded_to_s3(self):
        return bool(self.s3_file_key)

    def generate_content_hash(self):
        if self.uploaded_to_s3:
            return

        super().generate_content_hash()

    def validate_url(self):
        if self.uploaded_to_s3:
            return

        super().validate_url()

    def get_content(self):
        if not self.uploaded_to_s3:
            return super().get_content()

        s3 = S3Operations()
        s3.read_file(self.s3_file_key)
