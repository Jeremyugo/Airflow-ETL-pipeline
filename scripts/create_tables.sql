-- Database Schema

-- Creating StoreData table
CREATE TABLE "storedata" (
    "Row ID" VARCHAR(36) NOT NULL,
    "Order ID" VARCHAR(36) NOT NULL,
    "Order Date" DATE NOT NULL,
    "Ship Date" DATE NOT NULL,
    "Delivery Duration" VARCHAR(36) NOT NULL,
    "Ship Mode" VARCHAR(36) NOT NULL,
    "Customer ID" VARCHAR(36) NOT NULL,
    "Segment" VARCHAR(36) NOT NULL,
    "City" VARCHAR(36) NOT NULL,
    "State" VARCHAR(36) NOT NULL,
    "Postal Code" VARCHAR(36),
    "Region" VARCHAR(36) NOT NULL,
    "Product ID" VARCHAR(36) NOT NULL,
    "Category" VARCHAR(36) NOT NULL,
    "Sub-Category" VARCHAR(36) NOT NULL,
    "Product Name" VARCHAR(256) NOT NULL,
    "Sales" REAL NOT NULL,
    "Quantity" INTEGER NOT NULL,
    "Discount" REAL NOT NULL,
    "Profit" REAL NOT NULL
);