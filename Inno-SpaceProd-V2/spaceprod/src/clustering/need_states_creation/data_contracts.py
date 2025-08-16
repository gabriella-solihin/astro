"""
These simple data contracts map the
'source column names' -> 'internal column names'
to ensure we can always be confident that we have right columns whenever
we read external data
TODO: will incorporate column types
TODO: will become part of the IO layer / interface once we implement it
"""

DATA_CONTRACT_POG_SECTIONS = {
    "RETAILER NAME": "RETAILER_NAME",
    "STORE": "STORE",
    "STORE NAME": "STORE_NAME",
    "STATUS": "STATUS",
    "RELEASE DATE": "RELEASE_DATE",
    "TERMINATION DATE": "TERMINATION_DATE",
    "PUBLISH DATE": "PUBLISH_DATE",
    "SECTION NAME": "SECTION_NAME",
    "UPC": "UPC",
    "STOCKCODE": "STOCKCODE",
    "LONG DESCRIPTION": "LONG_DESCRIPTION",
    "UOM": "UOM",
    "CASEPACK": "CASEPACK",
    "SHELF NUMBER": "SHELF_NUMBER",
    "POSITION": "POSITION",
    "FACINGS": "FACINGS",
    "CAPACITY": "CAPACITY",
    "HEIGHT": "HEIGHT",
    "DEPTH": "DEPTH",
    "WIDTH": "WIDTH",
    "LENGTH, SECTION": "LENGTH_SECTION",
    "LENGTH, SECTION FOOTAGE": "LENGTH_SECTION_FOOTAGE",
}

DATA_CONTRACT_POG_SECTION_MASTER = {
    "SECTION NAME": "SECTION_NAME",
    "SECTION MASTER": "SECTION_MASTER",
}

DATA_CONTRACT_APOLLO = {
    "Retailer": "RETAILER",
    "RetailerName": "RETAILER_NAME",
    "Store": "STORE",
    "StoreName": "STORE_NAME",
    "ReleaseDate": "RELEASE_DATE",
    "TerminationDate": "TERMINATION_DATE",
    "PublishDate": "PUBLISH_DATE",
    "SectionName": "SECTION_NAME",
    "UPC": "UPC",
    "Stockcode": "STOCKCODE",
    "LongDescription": "LONG_DESCRIPTION",
    "UOM": "UOM",
    "Casepack": "CASEPACK",
    "ShelfNumber": "SHELF_NUMBER",
    "Position": "POSITION",
    "Facings": "FACINGS",
    "Capacity": "CAPACITY",
    "Height": "HEIGHT",
    "Depth": "DEPTH",
    "Width": "WIDTH",
    "LengthSection": "LENGTH_SECTION_INCHES",
    "STATUS": "STATUS",
    "LengthSectionFeet": "LENGTH_SECTION_FEET",
}

DATA_CONTRACT_SPACEMAN = {
    "SobeysPlanoPerso_StoreID": "STORE",
    "SobeysPlanoKey_Planogram": "SECTION_NAME",
    "SobeysPlanoKey_ModifiedDate": "RELEASE_DATE",  # TODO: check
    "SobeysPlanoPerso_ArticleNmbr": "STOCKCODE",
    "Position_HorizontalFacing": "FACINGS",
    "Product_Width": "WIDTH",
    "Product_Height": "HEIGHT",
    "Product_Depth": "DEPTH",
    "SobeysPlanoPerso_ShelfNmbr": "SHELF_NUMBER",
    "SobeysPlanoPerso_PlanoLength": "LENGTH_SECTION_INCHES",
}
