from transformation_utils.create_dim_counterparty import create_dim_counterparty
from transformation_utils.create_dim_currency import create_dim_currency
from transformation_utils.create_dim_date import create_dim_date
from transformation_utils.create_dim_location import create_dim_location
from transformation_utils.create_dim_staff import create_dim_staff
from transformation_utils.create_dim_design import create_dim_design
from transformation_utils.create_fact_sales_order import create_fact_sales_order
from transformation_utils.create_fact_payment import create_fact_payment
from transformation_utils.create_fact_purchase_order import create_fact_purchase_order
from transformation_utils.create_dim_transaction import create_dim_transaction
from transformation_utils.create_dim_payment_type import create_dim_payment_type
from utils.get_bucket_objects import get_bucket_objects
from utils.get_bucket_name import get_bucket_name
from utils.get_folder_with_files import get_folder_with_files
from utils.renaming_folders import rename_word_in_folder_name
from utils.move_s3_objects import move_s3_objects_to_new_folder
import logging


def get_file_folder_name(end_prefix, files):
    folder_name = [file for file in files if end_prefix in file]

    if len(folder_name) > 0:
        return folder_name[0]
    else:
        return None


def transform(folder_name, files, extraction_bucket, processed_bucket):

    logger = logging.getLogger('Transformation')

    address = get_file_folder_name("address.parquet", files)
    counterparty = get_file_folder_name("counterparty.parquet", files)
    department = get_file_folder_name("department.parquet", files)
    design = get_file_folder_name("design.parquet", files)
    payment_type = get_file_folder_name("payment_type.parquet", files)
    payment = get_file_folder_name("payment.parquet", files)
    purchase_order = get_file_folder_name(
        "purchase_order.parquet", files)
    sales_order = get_file_folder_name("sales_order.parquet", files)
    staff = get_file_folder_name("staff.parquet", files)
    currency = get_file_folder_name("currency.parquet", files)
    staff = get_file_folder_name("staff.parquet", files)
    transaction = get_file_folder_name("transaction.parquet", files)

    processed_folder_name = rename_word_in_folder_name(
        folder_name, "extraction", "processed")

    if sales_order:
        create_fact_sales_order(
            processed_folder_name,
            sales_order,
            extraction_bucket,
            processed_bucket)

    if address:
        create_dim_location(
            processed_folder_name,
            address,
            extraction_bucket,
            processed_bucket)

    if counterparty and address:
        create_dim_counterparty(
            processed_folder_name,
            counterparty,
            address,
            extraction_bucket,
            processed_bucket)

    if staff and department:
        create_dim_staff(
            processed_folder_name,
            staff,
            department,
            extraction_bucket,
            processed_bucket)

    if design:
        create_dim_design(processed_folder_name, design,
                          extraction_bucket, processed_bucket)
    if currency:
        create_dim_currency(
            processed_folder_name,
            currency,
            extraction_bucket,
            processed_bucket)

    if sales_order or payment or purchase_order:
        create_dim_date(
            processed_folder_name,
            sales_order,
            payment,
            purchase_order,
            extraction_bucket,
            processed_bucket)

    if payment:
        create_fact_payment(processed_folder_name, payment,
                            extraction_bucket, processed_bucket)

    if purchase_order:
        create_fact_purchase_order(processed_folder_name, purchase_order,
                                   extraction_bucket, processed_bucket)

    if transaction:
        create_dim_transaction(processed_folder_name, transaction,
                               extraction_bucket, processed_bucket)

    if payment_type:
        create_dim_payment_type(processed_folder_name, payment_type,
                                extraction_bucket, processed_bucket)

    logger.info(f"Folder {folder_name} has been transformed.")

    new_folder_name = rename_word_in_folder_name(
        folder_name, "extraction", "transformed")

    move_s3_objects_to_new_folder(
        files,
        folder_name,
        new_folder_name,
        extraction_bucket)


def handler(event, context):

    logger = logging.getLogger('Transformation')

    # get bucket
    extraction_bucket = get_bucket_name("nc-project-ingestion-zone-")
    processed_bucket = get_bucket_name("nc-project-processed-data-")

    files_in_extraction_bucket = get_bucket_objects(extraction_bucket)
    # grouping files by folder name but only untransformed data
    untransformed_dictionary = get_folder_with_files(
        "transformed", files_in_extraction_bucket)

    if len(untransformed_dictionary) == 0:
        logger.info(
            "There are no untransformed files in the extraction bucket.")
        return

    # Transforming contents in each folder
    for folder_name, files in untransformed_dictionary.items():
        transform(folder_name, files, extraction_bucket, processed_bucket)


if __name__ == "__main__":
    handler('helllo', 'world')
