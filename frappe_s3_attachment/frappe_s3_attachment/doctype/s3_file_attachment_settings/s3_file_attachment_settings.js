// Copyright (c) 2018, Frappe and contributors
// For license information, please see license.txt

frappe.ui.form.on("S3 File Attachment Settings", {
    migrate_existing_files(frm) {
        frappe.call({
            method: "frappe_s3_attachment.controller.migrate_existing_files",
        });
    },
});
