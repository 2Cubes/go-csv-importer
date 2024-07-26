package main

import (
	"context"
	"encoding/csv"
	"errors"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
	"github.com/shopspring/decimal"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

const batchSize = 1000

type Product struct {
	SKU           string
	Name          string
	Description   string
	Price         decimal.Decimal
	Stock         int
	Brand         string
	Category      string
	Subcategory   string
	BrandID       int
	CategoryID    int
	SubcategoryID int
}

func mapCSVRecord(record []string) (*Product, error) {
	if len(record) < 9 {
		return nil, errors.New("invalid record length")
	}

	stock := 0
	if record[5] == "Да" {
		stock = 1
	} else if record[5] == "Нет" {
		stock = 0
	} else {
		return nil, errors.New("invalid stock value")
	}

	priceStr := strings.Replace(record[3], ",", ".", -1)
	price, err := decimal.NewFromString(priceStr)
	if err != nil {
		return nil, err
	}

	return &Product{
		SKU:         record[2],
		Name:        record[1],
		Description: record[10],
		Price:       price,
		Stock:       stock,
		Brand:       record[6],
		Category:    record[7],
		Subcategory: record[8],
	}, nil
}

func openCSVFile(filePath string) (*csv.Reader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(transform.NewReader(file, charmap.Windows1251.NewDecoder()))
	reader.Comma = ';'
	return reader, nil
}

func insertBrandsAndCategories(brandSet map[string]struct{}, categorySet map[string]string, dbpool *pgxpool.Pool) (map[string]int, map[string]int, error) {
	brandMap := make(map[string]int)
	categoryMap := make(map[string]int)

	tx, err := dbpool.Begin(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(context.Background())

	for brand := range brandSet {
		var brandID int
		err = tx.QueryRow(context.Background(), `
			INSERT INTO brands (name) VALUES ($1)
			ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
			RETURNING id
		`, brand).Scan(&brandID)
		if err != nil {
			return nil, nil, err
		}
		log.Printf("Inserted brand: %s with ID %d", brand, brandID)
		brandMap[brand] = brandID
	}

	for category, parentCategory := range categorySet {
		var categoryID int
		if parentCategory == "" {
			err = tx.QueryRow(context.Background(), `
				INSERT INTO categories (name, parent_id) VALUES ($1, NULL)
				ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
				RETURNING id
			`, category).Scan(&categoryID)
			if err != nil {
				return nil, nil, err
			}
		} else {
			var parentCategoryID int
			err = tx.QueryRow(context.Background(), `
				INSERT INTO categories (name, parent_id) VALUES ($1, NULL)
				ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
				RETURNING id
			`, parentCategory).Scan(&parentCategoryID)
			if err != nil {
				return nil, nil, err
			}
			err = tx.QueryRow(context.Background(), `
				INSERT INTO categories (name, parent_id) VALUES ($1, $2)
				ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
				RETURNING id
			`, category, parentCategoryID).Scan(&categoryID)
			if err != nil {
				return nil, nil, err
			}
		}
		categoryMap[category] = categoryID
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return nil, nil, err
	}

	return brandMap, categoryMap, nil
}

func processBatch(batch []*Product, dbpool *pgxpool.Pool, wg *sync.WaitGroup) {
	defer wg.Done()

	tx, err := dbpool.Begin(context.Background())
	if err != nil {
		log.Printf("Unable to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(context.Background())

	batchQueries := &pgx.Batch{}
	for _, product := range batch {
		batchQueries.Queue("INSERT INTO products (sku, name, description, price, stock, brand_id, category_id) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (sku) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, price = EXCLUDED.price, stock = EXCLUDED.stock, brand_id = EXCLUDED.brand_id, category_id = EXCLUDED.category_id",
			product.SKU, product.Name, product.Description, product.Price, product.Stock, product.BrandID, product.CategoryID)
	}

	br := tx.SendBatch(context.Background(), batchQueries)
	if _, err := br.Exec(); err != nil {
		log.Printf("Error executing batch: %v", err)
		return
	}
	br.Close()

	err = tx.Commit(context.Background())
	if err != nil {
		log.Printf("Unable to commit transaction: %v", err)
	}
}

func main() {

	file, err := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("error opening log file: %v", err)
	}
	defer file.Close()
	log.SetOutput(file)

	err = godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL not set in .env file")
	}

	dbpool, err := pgxpool.Connect(context.Background(), connStr)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbpool.Close()

	fileInfo, err := os.Stat("data/largefile.csv")
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}

	lastModified := fileInfo.ModTime()

	var lastSync time.Time
	err = dbpool.QueryRow(context.Background(), "SELECT last_sync_time FROM sync_info ORDER BY id DESC LIMIT 1").Scan(&lastSync)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			log.Println("No rows in sync_info, proceeding with import")
			lastSync = time.Time{} // Установим lastSync в начальное значение времени
		} else {
			log.Fatalf("Error querying sync_info: %v", err)
		}
	}

	if lastModified.After(lastSync) {
		log.Println("Starting import...")

		reader, err := openCSVFile("data/largefile.csv")
		if err != nil {
			log.Fatalf("Error opening CSV file: %v", err)
		}

		_, err = reader.Read()
		if err != nil {
			log.Fatalf("Error reading CSV header: %v", err)
		}

		brandSet := make(map[string]struct{})
		categorySet := make(map[string]string)
		var records []*Product

		for {
			record, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				log.Fatalf("Error reading CSV file: %v", err)
			}

			product, err := mapCSVRecord(record)
			if err != nil {
				log.Printf("Skipping invalid record: %v", err)
				continue
			}

			brandSet[product.Brand] = struct{}{}
			categorySet[product.Category] = ""

			if product.Subcategory != "" {
				log.Printf("Subcategory: %s", product.Subcategory)
				categorySet[product.Subcategory] = product.Category
			}

			records = append(records, product)
		}

		brandMap, categoryMap, err := insertBrandsAndCategories(brandSet, categorySet, dbpool)
		if err != nil {
			log.Fatalf("Error inserting brands and categories: %v", err)
		}

		reader, err = openCSVFile("data/largefile.csv")
		if err != nil {
			log.Fatalf("Error opening CSV file: %v", err)
		}

		_, err = reader.Read()
		if err != nil {
			log.Fatalf("Error reading CSV header: %v", err)
		}

		records = records[:0]

		for {
			record, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				log.Fatalf("Error reading CSV file: %v", err)
			}

			product, err := mapCSVRecord(record)
			if err != nil {
				log.Printf("Skipping invalid record: %v", err)
				continue
			}

			product.BrandID = brandMap[product.Brand]

			if product.Subcategory != "" {
				product.CategoryID = categoryMap[product.Subcategory]
				log.Printf("Processing product: %s with subcategory: %s", product.SKU, product.Subcategory)
			} else {
				product.CategoryID = categoryMap[product.Category]
			}

			records = append(records, product)
		}

		var wg sync.WaitGroup

		for i := 0; i < len(records); i += batchSize {
			end := i + batchSize
			if end > len(records) {
				end = len(records)
			}

			batch := records[i:end]
			wg.Add(1)
			go processBatch(batch, dbpool, &wg)
		}

		wg.Wait()

		_, err = dbpool.Exec(context.Background(), "INSERT INTO sync_info (last_sync_time) VALUES ($1)", lastModified)
		if err != nil {
			log.Fatalf("Error updating sync_info: %v", err)
		}

		log.Println("Import finished successfully")
	} else {
		log.Println("No updates found, skipping import")
	}
}
