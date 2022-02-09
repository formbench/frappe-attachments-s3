from __future__ import unicode_literals

import random
import string
import datetime
import re
import os

import magic
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

import frappe
from frappe.utils import get_site_path

from frappe_s3_attachment.utils import strip_special_chars


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
            params.aws_secret_access_key = self.settings.aws_secret

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
                    return key.rstrip("/").lstrip("/")
            except Exception:
                pass

        file_name = file_name.replace(" ", "_")
        file_name = strip_special_chars(file_name)
        hash = "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(8)
        )

        today = datetime.datetime.now()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        doc_path = None
        try:
            doc_path = frappe.db.get_value(
                parent_doctype,
                filters={"name": parent_name},
                fieldname=["s3_folder_path"],
            )
            doc_path = doc_path.rstrip("/").lstrip("/")
        except Exception as e:
            print(e)

        if doc_path:
            return f"{doc_path}/{hash}_{file_name}"

        # TODO: confirm this
        if not parent_doctype:
            parent_doctype = "All"

        key = f"{year}/{month}/{day}/{parent_doctype}/{hash}_{file_name}"
        if self.settings.folder_name:
            key = self.settings.folder_name + key
        return key

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

    def get_file_url(self, key, file_name=None):
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


def upload_file_to_s3(doc, method=None):
    """
    check and upload files to s3. the path check and
    """
    s3 = S3Operations()
    exclude_doctypes = frappe.local.conf.get("ignore_s3_upload_for_doctype") or [
        "Data Import"
    ]

    if not s3.settings.is_enabled() or doc.attached_to_doctype in exclude_doctypes:
        return

    file_url = doc.file_url.lstrip("/")
    if not doc.is_private:
        file_url = f"public/{file_url}"

    file_path = get_site_path(file_url)
    key = s3.upload_file(
        file_path,
        doc.file_name,
        doc.is_private,
        doc.attached_to_doctype,
        doc.attached_to_name,
    )

    if doc.is_private:
        method_name = "frappe_s3_attachment.controller.generate_file"
        file_url = """/api/method/{0}?key={1}&file_name={2}""".format(
            method_name, key, doc.file_name
        )
    else:
        file_url = "{}/{}/{}".format(
            s3.s3_client.meta.endpoint_url, s3.settings.bucket_name, key
        )

    doc.db_set(
        {
            "file_url": file_url,
            "folder": "Home/Attachments",
            "old_parent": "Home/Attachments",
            "content_hash": "",
            "s3_file_key": key,
        }
    )
    os.remove(file_path)

    if not doc.attached_to_doctype:
        return

    parent_image_field = frappe.get_meta(doc.attached_to_doctype).get("image_field")
    if not parent_image_field:
        return

    frappe.db.set_value(
        doc.attached_to_doctype, doc.attached_to_name, parent_image_field, file_url
    )


@frappe.whitelist()
def generate_file(key=None, file_name=None):
    """
    Function to stream file from s3.
    """

    if not key:
        frappe.local.response["body"] = "Key not found."
        return

    s3_upload = S3Operations()
    frappe.local.response["location"] = s3_upload.get_file_url(key, file_name)
    frappe.local.response["type"] = "redirect"


# TODO: check following code
def upload_existing_files_s3(name, file_name):
    """
    Function to upload all existing files.
    """
    file_doc_name = frappe.db.get_value("File", {"name": name})
    if file_doc_name:
        doc = frappe.get_doc("File", name)
        s3_upload = S3Operations()
        path = doc.file_url
        site_path = frappe.utils.get_site_path()
        parent_doctype = doc.attached_to_doctype
        parent_name = doc.attached_to_name
        if not doc.is_private:
            file_path = site_path + "/public" + path
        else:
            file_path = site_path + path
        key = s3_upload.upload_file(
            file_path, doc.file_name, doc.is_private, parent_doctype, parent_name
        )

        if doc.is_private:
            method = "frappe_s3_attachment.controller.generate_file"
            file_url = """/api/method/{0}?key={1}""".format(method, key)
        else:
            file_url = "{}/{}/{}".format(
                s3_upload.s3_client.meta.endpoint_url, s3_upload.BUCKET, key
            )
        os.remove(file_path)
        doc = frappe.db.sql(
            """UPDATE `tabFile` SET file_url=%s, folder=%s,
            old_parent=%s, content_hash=%s WHERE name=%s""",
            (file_url, "Home/Attachments", "Home/Attachments", key, doc.name),
        )
        frappe.db.commit()
    else:
        pass


def s3_file_regex_match(file_url):
    """
    Match the public file regex match.
    """
    return re.match(
        r"^(https:|/api/method/frappe_s3_attachment.controller.generate_file)", file_url
    )


@frappe.whitelist()
def migrate_existing_files():
    """
    Function to migrate the existing files to s3.
    """
    # get_all_files_from_public_folder_and_upload_to_s3
    files_list = frappe.get_all("File", fields=["name", "file_url", "file_name"])
    for file in files_list:
        if file["file_url"]:
            if not s3_file_regex_match(file["file_url"]):
                upload_existing_files_s3(file["name"], file["file_name"])
    return True


def delete_file_from_s3(doc, method):
    """Delete file from s3"""
    s3 = S3Operations()
    s3.delete_file(doc.s3_file_key)
