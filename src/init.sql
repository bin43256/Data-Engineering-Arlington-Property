CREATE TABLE IF NOT EXISTS dim_sale_date (
    saleDateKey INT PRIMARY KEY,
    saleDate DATE,
    year INT,
    Month INT,
    day INT,
    weekday INT,
    quarter INT
);

CREATE TABLE IF NOT EXISTS dim_sales_type (
    salesTypeKey INT PRIMARY KEY,
    salesTypeCode VARCHAR(255),
    salesTypeDsc VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_dwellings_general (
    dwellingKey INT PRIMARY KEY,
    realEstatePropertyCode INT,
    coolingTypeDsc VARCHAR(255),
    dwellingTypeDsc VARCHAR(255),
    heatingTypeDsc VARCHAR(255),
    dwellingYearBuiltDate INT,
    storiesQuantityDsc VARCHAR(255),
    fireplaceCnt INT
);

CREATE TABLE IF NOT EXISTS dim_dwellings_interior (
    improvementInteriorKey INT PRIMARY KEY,
    realEstatePropertyCode INT,
    dwellingKey INT,
    baseAreaSquareFeetQty INT,
    bedroomCnt INT,
    finishedAreaSquareFeetQty INT,
    twoFixtureBathroomCnt INT,
    threeFixtureBathroomCnt INT,
    fourFixtureBathroomCnt INT,
    fiveFixtureBathroomCnt INT,
    floorNbr VARCHAR(255),
    floorKey INT
);

CREATE TABLE IF NOT EXISTS dim_property_class (
    propertyClassTypeKey INT PRIMARY KEY,
    propertyClassTypeCode VARCHAR(255),
    propertyClassTypeDsc VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_property (
    propertyKey INT PRIMARY KEY,
    realEstatePropertyCode INT,
    legalDsc VARCHAR(255),
    lotSizeQty INT,
    propertyStreetNbrNameText VARCHAR(255),
    propertyUnitNbr VARCHAR(255),
    propertyCityName VARCHAR(255),
    propertyZipCode VARCHAR(255),
    propertyClassTypeKey INT
);

CREATE TABLE IF NOT EXISTS fact_sales (   
    salesHistoryKey INT PRIMARY KEY,
    realEstatePropertyCode INT,
    propertyKey INT,
    salesTypeKey INT,
    saleDateKey INT,
    FOREIGN KEY(salesTypeKey) REFERENCES dim_sales_type(salesTypeKey),
    FOREIGN KEY(saleDateKey) REFERENCES dim_sale_Date(saleDateKey),
    saleAmt DECIMAL
);