package ru.s0qva.selsuptesttask;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CrptApi {
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final BlockingQueue<Runnable> requestQueue;
    private final int requestLimit;
    private final long timeIntervalMillis;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.requestQueue = new LinkedBlockingQueue<>();
        this.requestLimit = requestLimit;
        this.timeIntervalMillis = timeUnit.toMillis(1);
        startRequestScheduler();
    }

    public static void main(String[] args) {
        CrptApi api = new CrptApi(SECONDS, 5);
        Document document = new Document(new Description("participantInn"),
                "docId",
                "docStatus",
                "docType",
                false,
                "ownerInn",
                "participantInn",
                "producerInn",
                LocalDate.now(),
                "productionType",
                new Product[]{new Product("cerificateDocument",
                        LocalDate.now(),
                        "certificateDocumentNumber",
                        "ownerInn",
                        "producerInn",
                        LocalDate.now(),
                        "tnverCode",
                        "uitCode",
                        "uituCoded")},
                LocalDate.now(),
                "regNumber");
        api.createDocument(document, "signature_or_whatever_it_is");
    }

    private void startRequestScheduler() {
        scheduler.scheduleAtFixedRate(() -> {
            for (int i = 0; i < requestLimit && !requestQueue.isEmpty(); i++) {
                Runnable requestTask = requestQueue.poll();
                if (requestTask != null) {
                    new Thread(requestTask).start();
                }
            }
        }, 0L, timeIntervalMillis, MILLISECONDS);
    }

    public void createDocument(Document document, String signature) {
        requestQueue.add(() -> {
            try {
                doCreateDocument(document, signature);
            } catch (IOException | InterruptedException exception) {
                exception.printStackTrace();
            }
        });
    }

    private void doCreateDocument(Document document, String signature) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://ismp.crpt.ru/api/v3/lk/documents/create"))
                .header("Content-Type", "application/json")
                .header("Signature", signature)
                .POST(BodyPublishers.ofString(objectMapper.writeValueAsString(document)))
                .build();
        // send the request to the external service
    }

    // It would be nice to include Lombok and get rid of the boilerplate + it let us use a @Builder annotation
    public static class Document {
        private final Description description;
        private final String docId;
        private final String docStatus;
        private final String docType;
        private final boolean importRequest;
        private final String ownerInn;
        private final String participantInn;
        private final String producerInn;
        private final LocalDate productionDate;
        private final String productionType;
        private final Product[] products;
        private final LocalDate regDate;

        private final String regNumber;

        @JsonCreator
        public Document(@JsonProperty(value = "description") Description description,
                        @JsonProperty(value = "doc_id") String docId,
                        @JsonProperty(value = "doc_status") String docStatus,
                        @JsonProperty(value = "doc_type") String docType,
                        @JsonProperty(value = "importRequest") boolean importRequest,
                        @JsonProperty(value = "owner_inn") String ownerInn,
                        @JsonProperty(value = "participant_inn") String participantInn,
                        @JsonProperty(value = "producer_inn") String producerInn,
                        @JsonProperty(value = "production_date") LocalDate productionDate,
                        @JsonProperty(value = "production_type") String productionType,
                        @JsonProperty(value = "products") Product[] products,
                        @JsonProperty(value = "reg_date") LocalDate regDate,
                        @JsonProperty(value = "reg_number") String regNumber) {
            this.description = description;
            this.docId = docId;
            this.docStatus = docStatus;
            this.docType = docType;
            this.importRequest = importRequest;
            this.ownerInn = ownerInn;
            this.participantInn = participantInn;
            this.producerInn = producerInn;
            this.productionDate = productionDate;
            this.productionType = productionType;
            this.products = products;
            this.regDate = regDate;
            this.regNumber = regNumber;
        }

        public Description getDescription() {
            return description;
        }

        public String getDocId() {
            return docId;
        }

        public String getDocStatus() {
            return docStatus;
        }

        public String getDocType() {
            return docType;
        }

        public boolean isImportRequest() {
            return importRequest;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public LocalDate getProductionDate() {
            return productionDate;
        }

        public String getProductionType() {
            return productionType;
        }

        public Product[] getProducts() {
            return products;
        }

        public LocalDate getRegDate() {
            return regDate;
        }

        public String getRegNumber() {
            return regNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Document document = (Document) o;
            return importRequest == document.importRequest
                    && Objects.equals(description, document.description)
                    && Objects.equals(docId, document.docId)
                    && Objects.equals(docStatus, document.docStatus)
                    && Objects.equals(docType, document.docType)
                    && Objects.equals(ownerInn, document.ownerInn)
                    && Objects.equals(participantInn, document.participantInn)
                    && Objects.equals(producerInn, document.producerInn)
                    && Objects.equals(productionDate, document.productionDate)
                    && Objects.equals(productionType, document.productionType)
                    && Objects.deepEquals(products, document.products)
                    && Objects.equals(regDate, document.regDate)
                    && Objects.equals(regNumber, document.regNumber);
        }

        @Override
        public int hashCode() {
            return Objects.hash(description,
                    docId,
                    docStatus,
                    docType,
                    importRequest,
                    ownerInn,
                    participantInn,
                    producerInn,
                    productionDate,
                    productionType,
                    Arrays.hashCode(products),
                    regDate,
                    regNumber);
        }

    }

    public static class Description {

        private final String participantInn;

        @JsonCreator
        public Description(@JsonProperty(value = "participantInn") String participantInn) {
            this.participantInn = participantInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Description that = (Description) o;
            return Objects.equals(participantInn, that.participantInn);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(participantInn);
        }

    }

    public static class Product {
        private final String certificateDocument;
        private final LocalDate certificateDocumentDate;
        private final String certificateDocumentNumber;
        private final String ownerInn;
        private final String producerInn;
        private final LocalDate productionDate;
        private final String tnvedCode;
        private final String uitCode;

        private final String uituCode;

        @JsonCreator
        public Product(@JsonProperty(value = "certificate_document") String certificateDocument,
                       @JsonProperty(value = "certificate_document_date") LocalDate certificateDocumentDate,
                       @JsonProperty(value = "certificate_document_number") String certificateDocumentNumber,
                       @JsonProperty(value = "owner_inn") String ownerInn,
                       @JsonProperty(value = "producer_inn") String producerInn,
                       @JsonProperty(value = "production_date") LocalDate productionDate,
                       @JsonProperty(value = "tnved_code") String tnvedCode,
                       @JsonProperty(value = "uit_code") String uitCode,
                       @JsonProperty(value = "uitu_code") String uituCode) {
            this.certificateDocument = certificateDocument;
            this.certificateDocumentDate = certificateDocumentDate;
            this.certificateDocumentNumber = certificateDocumentNumber;
            this.ownerInn = ownerInn;
            this.producerInn = producerInn;
            this.productionDate = productionDate;
            this.tnvedCode = tnvedCode;
            this.uitCode = uitCode;
            this.uituCode = uituCode;
        }

        public String getCertificateDocument() {
            return certificateDocument;
        }

        public LocalDate getCertificateDocumentDate() {
            return certificateDocumentDate;
        }

        public String getCertificateDocumentNumber() {
            return certificateDocumentNumber;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public LocalDate getProductionDate() {
            return productionDate;
        }

        public String getTnvedCode() {
            return tnvedCode;
        }

        public String getUitCode() {
            return uitCode;
        }

        public String getUituCode() {
            return uituCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Product product = (Product) o;
            return Objects.equals(certificateDocument, product.certificateDocument)
                    && Objects.equals(certificateDocumentDate, product.certificateDocumentDate)
                    && Objects.equals(certificateDocumentNumber, product.certificateDocumentNumber)
                    && Objects.equals(ownerInn, product.ownerInn)
                    && Objects.equals(producerInn, product.producerInn)
                    && Objects.equals(productionDate, product.productionDate)
                    && Objects.equals(tnvedCode, product.tnvedCode)
                    && Objects.equals(uitCode, product.uitCode)
                    && Objects.equals(uituCode, product.uituCode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(certificateDocument,
                    certificateDocumentDate,
                    certificateDocumentNumber,
                    ownerInn,
                    producerInn,
                    productionDate,
                    tnvedCode,
                    uitCode,
                    uituCode);
        }

    }
}
