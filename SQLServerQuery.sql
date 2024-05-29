CREATE DATABASE data_transaction;
use data_transaction;

BEGIN TRANSACTION;
	
SAVE TRANSACTION SV1;

CREATE TABLE Transaction_Data (
    TransactionID varchar(12),
    TransactionDate DATE,
	ProductID varchar(12),
	ProductName varchar(50),
	Price DECIMAL(10, 2),
	Quantity INT,
    CustomerID INT,
	Country varchar(20)
);

BULK INSERT Transaction_Data
FROM 'C:\Users\lenovo\Documents\SQL Server Management Studio\normalisasi database\data_file\Transaction_data.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

SAVE TRANSACTION SV2;


-- Tabel Countries
CREATE TABLE Countries (
    CountryID INT IDENTITY(1,1) PRIMARY KEY,
    CountryName VARCHAR(20) UNIQUE
);

-- Tabel Products
CREATE TABLE Products (
    ProductID VARCHAR(12) PRIMARY KEY,
    ProductName VARCHAR(50),
    Price DECIMAL(10, 2)
);

-- Tabel Customers
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    CountryID INT,
    FOREIGN KEY (CountryID) REFERENCES Countries(CountryID)
);

-- Tabel Transactions
CREATE TABLE Transactions (
    TransactionID VARCHAR(12) PRIMARY KEY,
    TransactionDate DATE,
    CustomerID INT,
    CountryID INT,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (CountryID) REFERENCES Countries(CountryID)
);

-- Tabel TransactionDetails
CREATE TABLE TransactionDetails (
    TransactionDetailID INT PRIMARY KEY IDENTITY,
    TransactionID VARCHAR(12),
    ProductID VARCHAR(12),
    Quantity INT,
    FOREIGN KEY (TransactionID) REFERENCES Transactions(TransactionID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

SAVE TRANSACTION SV3;



BEGIN TRY
	-- Insert ke Countries
	WITH DistinctCountries AS (
		SELECT DISTINCT Country 
		FROM Transaction_Data
	)
	INSERT INTO Countries (CountryName)
	SELECT Country
	FROM DistinctCountries;


	-- Insert ke Products
	WITH MinPriceProducts AS (
		SELECT 
			ProductID, 
			ProductName, 
			MIN(Price) AS Price
		FROM 
			Transaction_Data
		GROUP BY 
			ProductID, 
			ProductName
	)
	INSERT INTO Products (ProductID, ProductName, Price)
	SELECT 
		ProductID, 
		ProductName, 
		Price
	FROM 
		MinPriceProducts;

	-- Insert ke Customers dengan CountryID
	WITH DistinctCustomers AS (
		SELECT DISTINCT CustomerID, Country 
		FROM Transaction_Data
	)
	INSERT INTO Customers (CustomerID, CountryID)
	SELECT 
		CustomerID,
		(SELECT CountryID FROM Countries WHERE CountryName = dc.Country)
	FROM DistinctCustomers dc;

	-- Insert ke Transactions dengan CountryID
	WITH DistinctTransactions AS (
		SELECT DISTINCT TransactionID, TransactionDate, CustomerID, Country 
		FROM Transaction_Data
	)
	INSERT INTO Transactions (TransactionID, TransactionDate, CustomerID, CountryID)
	SELECT 
		TransactionID, 
		TransactionDate, 
		CustomerID,
		(SELECT CountryID FROM Countries WHERE CountryName = dt.Country)
	FROM DistinctTransactions dt;

	-- Insert ke TransactionDetails
	INSERT INTO TransactionDetails (TransactionID, ProductID, Quantity)
	SELECT 
		TransactionID,
		ProductID,
		Quantity
	FROM Transaction_Data;
END TRY
BEGIN CATCH

	ROLLBACK TRANSACTION SV3;
    PRINT 'Error occurred, rolled back to SavePoint5';

END CATCH;

SAVE TRANSACTION SV4;

COMMIT;

