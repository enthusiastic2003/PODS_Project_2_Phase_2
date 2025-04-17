// package com.example;


// //#test-top
// import akka.actor.typed.ActorRef;
// import akka.http.javadsl.model.*;
// import akka.http.javadsl.testkit.JUnitRouteTest;
// import akka.http.javadsl.testkit.TestRoute;
// import org.junit.*;
// import org.junit.runners.MethodSorters;
// import akka.http.javadsl.model.HttpRequest;
// import akka.http.javadsl.model.StatusCodes;
// import akka.actor.testkit.typed.javadsl.TestKitJunitResource;


// //#set-up
// @FixMethodOrder(MethodSorters.NAME_ASCENDING)
// public class ProductRoutesTest extends JUnitRouteTest {

//     @ClassRule
//     public static TestKitJunitResource testkit = new TestKitJunitResource();

//     //#test-top
//     // shared registry for all tests
//     private static ActorRef<ProductRegistry.Command> productRegistry;
//     private TestRoute appRoute;

//     @BeforeClass
//     public static void beforeClass() {
//         productRegistry = testkit.spawn(ProductRegistry.create());
//     }

//     @Before
//     public void before() {
//         ProductRoutes productRoutes = new ProductRoutes(testkit.system(), productRegistry);
//         appRoute = testRoute(productRoutes.productRoutes());
//     }

//     @AfterClass
//     public static void afterClass() {
//         testkit.stop(productRegistry);
//     }

//     //#set-up
//     //#actual-test
//     @Test
//     public void test1NoProducts() {
//         appRoute.run(HttpRequest.GET("/products"))
//                 .assertStatusCode(StatusCodes.OK)
//                 .assertMediaType("application/json")
//                 .assertEntity("{\"products\":[]}");
//     }

//     //#actual-test
//     //#testing-post
//     @Test
//     public void test2HandlePOST() {
//         appRoute.run(HttpRequest.POST("/products")
//                 .withEntity(MediaTypes.APPLICATION_JSON.toContentType(),
//                         "{\"name\": \"Kapi\", \"age\": 42, \"countryOfResidence\": \"jp\"}"))
//                 .assertStatusCode(StatusCodes.CREATED)
//                 .assertMediaType("application/json")
//                 .assertEntity("{\"description\":\"Product Kapi created.\"}");
//     }
//     //#testing-post

//     @Test
//     public void test3Remove() {
//         appRoute.run(HttpRequest.DELETE("/products/Kapi"))
//                 .assertStatusCode(StatusCodes.OK)
//                 .assertMediaType("application/json")
//                 .assertEntity("{\"description\":\"Product Kapi deleted.\"}");

//     }
//     //#set-up
// }
// //#set-up
