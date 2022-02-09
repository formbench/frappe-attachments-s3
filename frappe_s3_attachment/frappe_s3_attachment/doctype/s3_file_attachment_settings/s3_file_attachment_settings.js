// Copyright (c) 2018, Frappe and contributors
// For license information, please see license.txt

frappe.ui.form.on("S3 File Attachment Settings", {
    async migrate_existing_files(frm) {
        frappe.throw("Migrating existing files...");

        frappe.msgprint("Local files getting migrated", "S3 Migration");
        const { message } = await frappe.call({
            method: "frappe_s3_attachment.controller.migrate_existing_files",
        });

        if (message) {
            frappe.msgprint("Upload Successful");
            location.reload(true);
        } else {
            frappe.msgprint("Retry");
        }
    },
});
