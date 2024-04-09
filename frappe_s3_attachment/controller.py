from __future__ import unicode_literals

import os
import re

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import magic
from urllib.parse import urlparse, parse_qs, quote, unquote

import frappe
from frappe import _, bold
from frappe.desk.doctype.bulk_update.bulk_update import show_progress
from frappe.utils import get_site_path, now_datetime

from frappe_s3_attachment.utils import strip_special_chars, strip_non_ascii


class S3Operations(object):
    def __init__(self):
        self.settings = frappe.get_doc("S3 File Attachment Settings")
        self.init_s3_client()

    def init_s3_client(self):
        """
        Initialise s3_client
        """
        params = frappe._dict(
            service_name="s3", config=Config(signature_version="s3v4")
        )
        if self.settings.endpoint_url:
            params.endpoint_url = self.settings.endpoint_url

        if self.settings.aws_key and self.settings.aws_secret:
            params.aws_access_key_id = self.settings.aws_key
            params.aws_secret_access_key = self.settings.get_password("aws_secret")

        if self.settings.region_name:
            params.region_name = self.settings.region_name

        self.s3_client = boto3.client(**params)

    def key_generator(self, file_name, parent_doctype, parent_name):
        """
        Generate keys for s3 objects uploaded with file name attached.
        """
        hook_cmd = frappe.get_hooks().get("s3_key_generator")
        if hook_cmd:
            try:
                key = frappe.get_attr(hook_cmd[0])(
                    file_name=file_name,
                    parent_doctype=parent_doctype,
                    parent_name=parent_name,
                )
                if key:
                    return key.strip("/")
            except Exception:
                pass

        file_name = f"{frappe.generate_hash(length=8)}_{file_name}"

        try:
            path = frappe.db.get_value(
                parent_doctype,
                filters={"name": parent_name},
                fieldname=["s3_folder_path"],
            )
            if path:
                return os.path.join(path.strip("/"), file_name)

        except Exception:
            # `s3_folder_path` field is not exist in the parent doctype
            pass

        return os.path.join(
            self.settings.folder_name or "",
            now_datetime().strftime("%Y/%m/%d"),
            parent_doctype or "",
            file_name,
        )

    def upload_file(
        self, file_path, file_name, is_private, parent_doctype, parent_name
    ):
        """
        Uploads a new file to S3.
        Strips the file extension to set the content_type in metadata.
        """
        mime_type = magic.from_file(file_path, mime=True)
        key = self.key_generator(file_name, parent_doctype, parent_name)
        content_type = mime_type
        try:
            extra_args = {
                "ContentType": content_type,
                "Metadata": {
                    "ContentType": content_type,
                },
            }

            if is_private:
                extra_args["Metadata"]["file_name"] = file_name
            else:
                extra_args["ACL"] = "public-read"

            self.s3_client.upload_file(
                file_path, self.settings.bucket_name, key, extra_args
            )

        except boto3.exceptions.S3UploadFailedError:
            frappe.throw(frappe._("File Upload Failed. Please try again."))

        return key

    def delete_file(self, key):
        """Delete file from s3"""

        if not key or not self.settings.delete_file_from_cloud:
            return

        try:
            self.s3_client.delete_object(Bucket=self.settings.bucket_name, Key=key)
        except ClientError:
            frappe.throw(frappe._("Access denied: Could not delete file"))

    def read_file(self, key):
        """
        Function to read file from a s3 file.
        """
        return self.s3_client.get_object(Bucket=self.settings.bucket_name, Key=key)

    def get_file_url(self, key, file_name=None, is_private=True):
        if is_private:
            method_name = "frappe_s3_attachment.controller.generate_file"
            return """/api/method/{0}?key={1}&file_name={2}""".format(
                method_name, key, file_name
            )

        return os.path.join(
            self.s3_client.meta.endpoint_url, self.settings.bucket_name, key
        )

    def get_signed_file_url(self, key, file_name=None):
        """
        Return url.

        :param bucket: s3 bucket name
        :param key: s3 object key
        """
        params = {
            "Bucket": self.settings.bucket_name,
            "Key": key,
        }

        if file_name:
            params["ResponseContentDisposition"] = f"filename={file_name}"

        return self.s3_client.generate_presigned_url(
            "get_object",
            Params=params,
            ExpiresIn=self.settings.get("signed_url_expiry_time") or 120,
        )

    def set_file_permission(self, key, private=True):
        """
        Set file permission.
        """
        try:
            self.s3_client.put_object_acl(
                Bucket=self.settings.bucket_name,
                Key=key,
                ACL="private" if private else "public-read",
            )
        except ClientError:
            frappe.throw(frappe._("Access denied: Could not change file permission"))


def upload_file_to_s3(doc, method=None):
    """
    check and upload files to s3.
    """
    # copied already uploaded File
    if is_s3_file_url(doc.file_url) and not doc.s3_file_key:
        return _link_file_to_s3(doc)

    """
    early return if doc is folder
    """
    if doc.is_folder:
         return

    _upload_file_to_s3(doc)


def _link_file_to_s3(file):
    """
    update `file_name` and `s3_file_key` by parsing `file_url`.
    """
    file_url = urlparse(file.file_url)
    query = parse_qs(file_url.query)

    if "file_name" in query and "key" in query:
        file.db_set(
            {
                "file_name": query["file_name"][0],
                "s3_file_key": query["key"][0],
            }
        )


def _upload_file_to_s3(file, s3=None):
    if isinstance(file, str):
        file = frappe.get_doc("File", file)

    if not s3:
        s3 = S3Operations()

    exclude_doctypes = frappe.local.conf.get("ignore_s3_upload_for_doctype") or [
        "Data Import"
    ]

    if not s3.settings.is_enabled() or file.attached_to_doctype in exclude_doctypes:
        return

    file_url = file.file_url.lstrip("/")
    if not file.is_private:
        file_url = f"public/{file_url}"

    safe_file_name  = strip_special_chars(file.file_name)
    file_name = strip_non_ascii(file.file_name)

    file_path = get_site_path(file_url)
    key = s3.upload_file(
        file_path,
        file_name,
        file.is_private,
        file.attached_to_doctype,
        file.attached_to_name,
    )

    file.update(
        {
            "file_url": s3.get_file_url(key, quote(safe_file_name), file.is_private),
            "content_hash": "",
            "s3_file_key": key,
        }
    )

    file.save()
    os.remove(file_path)

    if not file.attached_to_doctype:
        return

    # parent_image_field = frappe.get_meta(file.attached_to_doctype).get("image_field")
    # if not parent_image_field:
    #     return

    # frappe.db.set_value(
    #     file.attached_to_doctype, file.attached_to_name, parent_image_field, file_url
    # )
    file.reload()


@frappe.whitelist()
def generate_file(key=None, file_name=None):
    """
    Function to stream file from s3.
    """

    if not key:
        frappe.local.response["body"] = "Key not found."
        return

    s3_upload = S3Operations()
    frappe.local.response["location"] = s3_upload.get_signed_file_url(key, file_name)
    frappe.local.response["type"] = "redirect"


def is_s3_file_url(file_url):
    """
    Match the s3 file regex match.
    """
    return re.match(
        r"^(https:|/api/method/frappe_s3_attachment.controller.generate_file)",
        file_url,
    )


@frappe.whitelist()
def migrate_existing_files():
    """
    Function to migrate the existing files to s3.
    """
    files = frappe.get_all(
        "File",
        filters={
            "s3_file_key": ("is", "not set"),
            "file_url": ("like", "%/files/%"),
        },
        fields=("name", "file_name"),
        order_by="modified desc",
    )

    s3 = S3Operations()
    total = len(files)
    failed = 0
    for index, file in enumerate(files):
        show_progress(
            files,
            _("Migrating local files to S3"),
            index,
            _("Migrating {} ({}/{})").format(file.file_name, index, total),
        )
        try:
            _upload_file_to_s3(file.name, s3)
        except Exception:
            failed += 1
            continue

    frappe.msgprint(
        msg=_("{} files out of {} migrated successfully.").format(
            bold(total - failed), bold(total)
        ),
        title="Migration completed",
        indicator="green",
    )


def delete_file_from_s3(doc, method):
    """Delete file from s3"""
    s3 = S3Operations()
    s3.delete_file(doc.s3_file_key)
