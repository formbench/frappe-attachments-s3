from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_s3_attachment.controller import S3Operations, generate_file
from frappe_s3_attachment.patches.link_public_copies import execute as link_public_copies

KEY = "ci/2026/10/23/File/abcd1234_invoice.pdf"
USER_WITHOUT_ACCESS = "s3-no-access@example.com"
USER_WITH_COPY = "s3-copy-owner@example.com"
URL = "/api/method/frappe_s3_attachment.controller.generate_file?key={}&file_name={}"


class TestGenerateFile(FrappeTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        settings = frappe.get_single("S3 File Attachment Settings")
        # dummy credentials: signing a URL needs no network call
        settings.update(
            {
                # adds the s3_file_key column, which commits; keep it before the test data
                "enable_s3_file_attachment": 1,
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

        # the user's copy is the older row, so the check must look past the first match
        insert_s3_file(KEY, "invoice.pdf").db_set("owner", USER_WITH_COPY)
        # a private File on S3, owned by Administrator and attached to nothing
        insert_s3_file(KEY, "invoice.pdf")

    # after_insert refuses a File once it is inserted, so undo each test's rows
    def setUp(self):
        frappe.db.savepoint("s3_test")

    def tearDown(self):
        frappe.set_user("Administrator")
        frappe.db.rollback(save_point="s3_test")

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
                    "file_url": URL.format(KEY, "renamed.pdf"),
                }
            ).insert()

    def test_copy_under_another_name_is_linked_for_a_reader(self):
        frappe.set_user(USER_WITH_COPY)
        # a URL of its own: core refuses other users a copy of an existing private URL
        # before our check runs
        file = frappe.get_doc(
            {
                "doctype": "File",
                "file_name": "reader-copy.pdf",
                "file_url": URL.format(KEY, "reader-copy.pdf"),
            }
        ).insert()
        self.assertEqual(file.s3_file_key, KEY)

    def test_setting_a_key_on_save_needs_read_access(self):
        frappe.set_user(USER_WITHOUT_ACCESS)
        file = frappe.get_doc(
            {"doctype": "File", "file_url": "https://example.com/own.pdf"}
        ).insert()
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

    def test_a_key_with_markup_is_refused(self):
        # core would strip the tag after validate and store KEY
        frappe.set_user(USER_WITHOUT_ACCESS)
        with self.assertRaisesRegex(frappe.ValidationError, "Invalid S3 key"):
            frappe.get_doc(
                {
                    "doctype": "File",
                    "file_url": "https://example.com/markup.pdf",
                    "s3_file_key": f"<x>{KEY}</x>",
                }
            ).insert()

    def test_a_public_file_cannot_take_a_private_key(self):
        frappe.set_user(USER_WITH_COPY)
        with self.assertRaisesRegex(frappe.ValidationError, "private S3 object"):
            frappe.get_doc(
                {
                    "doctype": "File",
                    "file_url": "https://example.com/public.pdf",
                    "is_private": 0,
                    "s3_file_key": KEY,
                }
            ).insert()

    def test_a_key_differing_in_case_is_another_object(self):
        # MariaDB's collation matches the two keys, S3 does not
        variant = KEY.replace("invoice", "Invoice")
        insert_s3_file(variant, "Invoice.pdf")
        frappe.set_user(USER_WITH_COPY)
        with self.assertRaises(frappe.PermissionError):
            generate_file(key=variant, file_name="Invoice.pdf")

    def test_deleting_a_copy_keeps_the_object_for_the_others(self):
        key = "ci/2026/10/23/File/efgh5678_shared.pdf"
        first, last = (insert_s3_file(key, "shared.pdf") for _ in range(2))
        with patch.object(S3Operations, "delete_file") as delete_file:
            first.delete()
            delete_file.assert_not_called()
            last.delete()
        delete_file.assert_called_once_with(key)

    def test_making_a_copy_public_needs_write_access_to_the_others(self):
        frappe.set_user(USER_WITH_COPY)
        file = insert_s3_file(KEY, "public-copy.pdf")
        file.is_private = 0
        with patch.object(S3Operations, "set_file_permission") as set_file_permission:
            with self.assertRaisesRegex(frappe.PermissionError, "Not permitted"):
                file.save()
        set_file_permission.assert_not_called()

    def test_making_a_file_public_makes_only_its_copies_public(self):
        key = "ci/2026/10/23/File/ijkl9012_flyer.pdf"
        file, copy = (insert_s3_file(key, "flyer.pdf") for _ in range(2))
        # MariaDB's collation matches this key too
        other = insert_s3_file(key.replace("flyer", "Flyer"), "Flyer.pdf")
        file.is_private = 0
        with patch.object(S3Operations, "set_file_permission") as set_file_permission:
            file.save()
        set_file_permission.assert_called_once_with(key, 0)
        self.assertEqual(
            frappe.db.get_value("File", copy.name, ["is_private", "file_url"]),
            (0, file.file_url),
        )
        self.assertEqual(frappe.db.get_value("File", other.name, "is_private"), 1)

    def test_a_public_s3_url_is_linked_to_its_key(self):
        key = "ci/2026/10/23/File/mnop3456_logo.png"
        file_url = S3Operations().get_file_url(key, is_private=False)
        file = frappe.get_doc({"doctype": "File", "file_url": file_url}).insert()
        self.assertEqual(file.s3_file_key, key)

    def test_a_public_url_to_a_private_object_needs_read_access(self):
        frappe.set_user(USER_WITHOUT_ACCESS)
        file_url = S3Operations().get_file_url(KEY, is_private=False)
        with self.assertRaisesRegex(frappe.PermissionError, "Not permitted"):
            frappe.get_doc({"doctype": "File", "file_url": file_url}).insert()

    # core's attach_files_to_document makes a File with an http(s) URL public
    def test_a_public_url_cannot_open_a_private_object(self):
        frappe.set_user(USER_WITH_COPY)
        file_url = S3Operations().get_file_url(KEY, is_private=False)
        with self.assertRaisesRegex(frappe.ValidationError, "private S3 object"):
            frappe.get_doc(
                {"doctype": "File", "file_url": file_url, "is_private": 1}
            ).insert()

    def test_a_private_copy_cannot_move_to_an_http_url(self):
        frappe.set_user(USER_WITH_COPY)
        file = insert_s3_file(KEY, "moved.pdf")
        file.file_url = "https://example.com/moved.pdf"
        with self.assertRaisesRegex(frappe.ValidationError, "private S3 object"):
            file.save()

    def test_the_patch_links_existing_public_copies(self):
        key = "ci/2026/10/23/File/qrst7890_banner.png"
        file_url = S3Operations().get_file_url(key, is_private=False)
        file = frappe.get_doc({"doctype": "File", "file_url": file_url}).insert()
        file.db_set("s3_file_key", None)
        # a copy from before KEY's object was made private
        stale = frappe.get_doc(
            {"doctype": "File", "file_url": "https://example.com/stale.pdf"}
        ).insert()
        stale.db_set("file_url", S3Operations().get_file_url(KEY, is_private=False))
        link_public_copies()
        self.assertEqual(frappe.db.get_value("File", file.name, "s3_file_key"), key)
        self.assertFalse(frappe.db.get_value("File", stale.name, "s3_file_key"))


def insert_s3_file(key, file_name):
    return frappe.get_doc(
        {
            "doctype": "File",
            "file_name": file_name,
            "file_url": URL.format(key, file_name),
            "is_private": 1,
        }
    ).insert()
