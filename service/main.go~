package main

import (
	elastic "gopkg.in/olivere/elastic.v3"
	"fmt"
	"strings"
	"strconv"
	"net/http"
	"encoding/json"
	"log"
	"reflect"
	"github.com/pborman/uuid"
)

const (
	INDEX = "around"
	TYPE = "post"
	DISTANCE = "200km"
	PROJECT_ID = "aroundxc-209205"
	//BT_INSTANCE = "arond-post"
	ES_URL = "http://35.185.23.137:9200"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User string `json:"user"`
	Message string `json:"message"`
	Location Location `json:"location"`
}

func main() {
	//Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}
	//Use the IndexExists service to check if a specified index exists
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
	//create a new index         ``symbol is used for multiline string here
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`

		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("started-service")
	http.HandleFunc("/post", handlerPost)
	http.HandleFunc("/search", handlerSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
	//Parse from body of request to get a json object
	fmt.Println("Received one post request")
	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil {
		panic(err)
		return
	}
	id := uuid.New()
	//Save to ES
	saveToES(&p, id)
	fmt.Fprintf(w, "Post received: %s\n", p.Message)
}

//Save a post to ElasticSearch
func saveToES(p *Post, id string) {
	//Create a client
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	//save to index
	_, err = es_client.Index().
		  Index(INDEX).
		  Type(TYPE).
		  Id(id).
		  BodyJson(p).
		  Refresh(true).
		  Do()
	if err != nil {
		panic(err)
		return
	}

	fmt.Printf("Post is saved to Index: %s\n", p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	//range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Printf("Search received: %f %f %s\n", lat, lon, ran)

	//Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	//Define geo distance query as specified in
	//https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	searchResult, err := client.Search().
			     Index(INDEX).
			     Query(q).
			     Pretty(true).
			     Do()
	if err != nil {
		panic(err)
	}
	//searchResult is of type of SearchResult and return hits, suggestions,
	//and all kinds of other information from ElasticSearch
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	//Iterate over the hits in search result
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { //instance of
		p := item.(Post) //get the concrete type value
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
		//Keyword filtering based on keyword such as web spam etc.
		if !containsFilteredWords(&p.Message) {
			ps = append(ps, p)
		}
	}

	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)
}

func containsFilteredWords (s *string) bool {
	filteredWords := []string{
		"fuck",
		"nigger",
	}
	for _, word := range filteredWords {
		if strings.Contains(*s, word) {
			return true
		}
	}
	return false
}
