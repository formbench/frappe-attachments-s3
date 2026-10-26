import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_s3_attachment.controller import generate_file
from frappe_s3_attachment.frappe_s3_attachment.doctype.s3_file_attachment_settings.s3_file_attachment_settings import (
    setup_custom_fields,
)

KEY = "ci/2026/10/23/File/abcd1234_invoice.pdf"
USER_WITHOUT_ACCESS = "s3-no-access@example.com"
USER_WITH_COPY = "s3-copy-owner@example.com"


class TestGenerateFile(FrappeTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # adds a column, which commits; keep it before the test data
        setup_custom_fields()

        settings = frappe.get_single("S3 File Attachment Settings")
        # dummy credentials: signing a URL needs no network call
        settings.update(
            {
                "bucket_name": "ci-bucket",
                "aws_key": "ci",
                "aws_secret": "ci",
                "region_name": "eu-central-1",
            }
        )
        settings.save()

        for user in (USER_WITHOUT_ACCESS, USER_WITH_COPY):
            if not frappe.db.exists("User", user):
                frappe.get_doc(
                    {
                        "doctype": "User",
                        "email": user,
                        "first_name": "S3",
                        "send_welcome_email": 0,
                    }
                ).insert()

        file = {
            "doctype": "File",
            "file_name": "invoice.pdf",
            "file_url": f"/api/method/frappe_s3_attachment.controller.generate_file?key={KEY}&file_name=invoice.pdf",
            "is_private": 1,
        }
        # the user's copy is the older row, so the check must look past the first match
        frappe.get_doc(file).insert().db_set("owner", USER_WITH_COPY)
        # a private File on S3, owned by Administrator and attached to nothing
        frappe.get_doc(file).insert()

    def tearDown(self):
        frappe.set_user("Administrator")

    def test_user_without_read_access_is_refused(self):
        frappe.set_user(USER_WITHOUT_ACCESS)
        with self.assertRaises(frappe.PermissionError):
            generate_file(key=KEY, file_name="invoice.pdf")

    def test_permitted_user_is_redirected_to_a_signed_url(self):
        generate_file(key=KEY, file_name="invoice.pdf")
        self.assertEqual(frappe.local.response["type"], "redirect")
        self.assertIn("abcd1234_invoice.pdf", frappe.local.response["location"])

    def test_owner_of_a_copy_is_redirected(self):
        frappe.set_user(USER_WITH_COPY)
        generate_file(key=KEY, file_name="invoice.pdf")
        self.assertEqual(frappe.local.response["type"], "redirect")

    def test_unknown_key_is_not_found(self):
        with self.assertRaises(frappe.DoesNotExistError):
            generate_file(key="ci/2026/10/23/File/unknown.pdf")

    # the message tells our check apart from core's create-permission check
    def test_copy_under_another_name_needs_read_access(self):
        frappe.set_user(USER_WITHOUT_ACCESS)
        with self.assertRaisesRegex(frappe.PermissionError, "Not permitted"):
            frappe.get_doc(
                {
                    "doctype": "File",
                    "file_name": "renamed.pdf",
                    "file_url": f"/api/method/frappe_s3_attachment.controller.generate_file?key={KEY}&file_name=renamed.pdf",
                }
            ).insert()

    def test_copy_under_another_name_is_linked_for_a_reader(self):
        frappe.set_user(USER_WITH_COPY)
        # a URL of its own: core refuses other users a copy of an existing private URL before our check runs
        file = frappe.get_doc(
            {
                "doctype": "File",
                "file_name": "reader-copy.pdf",
                "file_url": f"/api/method/frappe_s3_attachment.controller.generate_file?key={KEY}&file_name=reader-copy.pdf",
            }
        ).insert()
        self.assertEqual(file.s3_file_key, KEY)

    def test_setting_a_key_on_save_needs_read_access(self):
        frappe.set_user(USER_WITHOUT_ACCESS)
        file = frappe.get_doc({"doctype": "File", "file_url": "https://example.com/own.pdf"}).insert()
        file.s3_file_key = KEY
        with self.assertRaisesRegex(frappe.PermissionError, "Not permitted"):
            file.save()

    def test_setting_a_key_needs_read_access(self):
        frappe.set_user(USER_WITHOUT_ACCESS)
        with self.assertRaisesRegex(frappe.PermissionError, "Not permitted"):
            frappe.get_doc(
                {
                    "doctype": "File",
                    "file_url": "https://example.com/invoice.pdf",
                    "s3_file_key": KEY,
                }
            ).insert()
