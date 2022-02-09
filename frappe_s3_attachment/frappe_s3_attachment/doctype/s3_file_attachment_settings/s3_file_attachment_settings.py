# -*- coding: utf-8 -*-
# Copyright (c) 2018, Frappe and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

from frappe.custom.doctype.custom_field.custom_field import create_custom_fields
from frappe.model.document import Document


class S3FileAttachmentSettings(Document):
    def is_enabled(self):
        return bool(self.enable_s3_file_attachment)

    def validate(self):
        if self.is_enabled():
            setup_custom_fields()


def setup_custom_fields():
    custom_fields = {
        "File": [
            dict(
                fieldname="s3_file_key",
                label="S3 File Key",
                fieldtype="Data",
                insert_after="content_hash",
                read_only=1,
                print_hide=1,
            )
        ],
    }

    create_custom_fields(custom_fields)
