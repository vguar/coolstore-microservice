package com.redhat.coolstore.service;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.redhat.coolstore.model.Product;
import org.bson.Document;


@ApplicationScoped
public class MongoCatalogService implements CatalogService {

    @Inject
    private MongoClient mc;

    @Inject
    Logger log;

    private MongoCollection<Document> productCollection;

	public MongoCatalogService() {
	}

	public List<Product> getProducts() {
        return StreamSupport.stream(productCollection.find().spliterator(), false)
                .map(d -> toProduct(d))
                .collect(Collectors.toList());

    }


    public void add(Product product) {
        productCollection.insertOne(toDocument(product));
    }

    public void addAll(List<Product> products) {
        List<Document> documents = products.stream().map(p -> toDocument(p)).collect(Collectors.toList());
        productCollection.insertMany(documents);
    }

    @PostConstruct
    protected void init() {
        log.info("@PostConstruct is called...");

        String dbName = System.getenv("DB_NAME");
        if(dbName==null || dbName.isEmpty()) {
            log.info("Could not get environment variable DB_NAME using the default value of 'CatalogDB'");
            dbName = "CatalogDB";
        }

        MongoDatabase db = mc.getDatabase(dbName);


        productCollection = db.getCollection("products");

        // Drop the collection if it exists and then add default content
        productCollection.drop();
        addAll(DEFAULT_PRODUCT_LIST);

    }

    @PreDestroy
    protected void destroy() {
        log.info("Closing MongoClient connection");
        if(mc!=null) {
            mc.close();
        }
    }

    /**
     * This method converts Product POJOs to MongoDB Documents, normally we would place this in a DAO
     * @param product
     * @return
     */
    private Document toDocument(Product product) {
        return new Document()
                .append("itemId",product.getItemId())
                .append("name",product.getName())
                .append("desc",product.getDesc())
                .append("price",product.getPrice());
    }

    /**
     * This method converts MongoDB Documents to Product POJOs, normally we would place this in a DAO
     * @param document
     * @return
     */
    private Product toProduct(Document document) {
        Product product =  new Product();
        product.setItemId(document.getString("itemId"));
        product.setName(document.getString("name"));
        product.setDesc(document.getString("desc"));
        product.setPrice(document.getDouble("price"));
        return product;
    }



    private static List<Product> DEFAULT_PRODUCT_LIST = new ArrayList<>();
    static {
        DEFAULT_PRODUCT_LIST.add(new Product("329299", "Carrelage Sol", "Carrelage sol et mur gris clair aspect bois l.15 x L.90 cm", 17.99));
        DEFAULT_PRODUCT_LIST.add(new Product("329199", "Applique Pop", "Applique, pop e14 Skit métal Jaune, 1 INSPIRE", 29.50));
        DEFAULT_PRODUCT_LIST.add(new Product("165613", "Poele a bois ADURO", "L'Aduro 9-3 Lux est étanche et raccordable à l'air extérieur.", 1590.00));
        DEFAULT_PRODUCT_LIST.add(new Product("165614", "Perceuse sans fil DEWALT", "Perceuse sans fil DEWALT Compact xr dck777d2t, 18 V 2 Ah, 2 batteries ", 199.00));
        DEFAULT_PRODUCT_LIST.add(new Product("165954", "Coffret d outils 108 pieces DEXTER", "11 tournevis, 1 tournevis porte-embout et ses embouts, 6 clés plates", 6.00));
        DEFAULT_PRODUCT_LIST.add(new Product("444434", "Seche serviettes ACOVA", "Sèche serviettes électrique ACOVA Plume titane 1000 W", 1253.00));
        DEFAULT_PRODUCT_LIST.add(new Product("444435", "Pistolet automatique GEOLIA Geot30", "Equipé de 7 formes de jets. Manche confortable. Anti-choc.", 12.65));
        DEFAULT_PRODUCT_LIST.add(new Product("444436", "Evier a encastrer", "Evier à encastrer résine noir Solo, 1 grand bac avec égouttoir ", 109.90));

    }

}
