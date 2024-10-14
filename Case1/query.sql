WITH 
tot_genre AS (
	SELECT 
		tra.GenreId,
		il.UnitPrice * SUM(il.Quantity) AS TotSalesGenre
	FROM Track tra
	INNER JOIN Genre gen ON tra.GenreId = gen.GenreId 
	INNER JOIN InvoiceLine il ON il.TrackId = tra.TrackId 
	GROUP BY tra.GenreId 
),
sales AS (
	SELECT 
		art.Name AS artist,
		tra.GenreId,
		gen.Name AS genre,
		il.UnitPrice * SUM(il.Quantity) AS sales,
		ROUND((il.UnitPrice * SUM(il.Quantity) / tg.TotSalesGenre) * 100, 1) AS sales_percentage_by_genre
	FROM Track tra 
	INNER JOIN Album alb ON alb.AlbumId = tra.AlbumId 
	INNER JOIN Genre gen ON gen.GenreId = tra.GenreId 
	INNER JOIN Artist art ON art.ArtistId = alb.ArtistId 
	INNER JOIN InvoiceLine il ON il.TrackId = tra.TrackId 
	INNER JOIN tot_genre tg ON tg.GenreId = tra.GenreId  
	GROUP BY 
		art.Name,
		tra.GenreId,
		gen.Name
)
SELECT 
	s.artist,
	s.genre,
	s.sales,
	CONCAT(s.sales_percentage_by_genre, "%") AS sales_percentage_by_genre,
	CONCAT(SUM(s.sales_percentage_by_genre) OVER (PARTITION BY s.genre ORDER BY s.sales_percentage_by_genre DESC, s.artist ASC), "%") AS cumulative_sum_by_genre
FROM sales s
ORDER BY s.genre asc, s.sales_percentage_by_genre DESC ;
