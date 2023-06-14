import entities.Book;
import entities.Borrow;
import entities.Card;
import queries.*;
import utils.ConnectConfig;
import utils.DatabaseConnector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.nio.file.Files;
import java.util.stream.Collectors;

public class Main {

    private static final Logger log = Logger.getLogger(Main.class.getName());
    private static LibraryManagementSystemImpl library;

    public static void main(String[] args) {
        try {
            // parse connection config from "resources/application.yaml"
            ConnectConfig conf = new ConnectConfig();
            log.info("Success to parse connect config. " + conf.toString());
            // connect to database
            DatabaseConnector connector = new DatabaseConnector(conf);
            library = new LibraryManagementSystemImpl(connector);
            boolean connStatus = connector.connect();
            if (!connStatus) {
                log.severe("Failed to connect database.");
                System.exit(1);
            }
            /* do somethings */

            System.out.println("-----------Welcome to Library Management System----------");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              1. Store Book (Single)                    |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              2. Store Book (From File)                 |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              3. Increase Book Stock                    |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              4. Remove Book                            |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              5. Modify Book Info                       |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              6. Query Book                             |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              7. Borrow Book                            |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              8. Return Book                            |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              9. Show Borrow History                    |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              10. Register Card                         |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              11. Remove Card                           |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              12. Show Cards                            |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              13. Reset Database                        |");
            System.out.println("---------------------------------------------------------");
            System.out.println("|              14. Exit                                  |");
            System.out.println("---------------------------------------------------------");
            System.out.print("\nPlease choose a option by its serial number: ");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            int choice = Integer.parseInt(br.readLine());
            while(choice != 14){
                if(choice == 1){
                    System.out.println("Please input attributes in order(each attribute in a new line): category title press publishYear author price stock");
                    String category = br.readLine();
                    String title = br.readLine();
                    String press = br.readLine();
                    int publishYear = Integer.parseInt(br.readLine());
                    String author = br.readLine();
                    Double price = Double.parseDouble(br.readLine());
                    int stock = Integer.parseInt(br.readLine());
                    Book b = new Book(category, title, press, publishYear, author, price, stock);
                    ApiResult rs = library.storeBook(b);
                    System.out.println(rs.message);
                } else if (choice == 2) {
                    System.out.println("Please input the file name (make sure the file is under the program directory)");
                    String str = br.readLine();
                    List<Book> books = new ArrayList<>();
                    try {
                        List<String> inputs = Files.lines(Paths.get(str)).collect(Collectors.toList());
                        /*Iterator<String> iterator = books.iterator();
                        while(iterator.hasNext()){
                            //String info = iterator.next();
                            Book book = new Book();
                            book.setCategory(iterator.next());
                            book.setTitle(iterator.next());
                        }*/
                        for (int i = 0; i < inputs.size(); i = i + 7) {
                            Book book = new Book();
                            book.setCategory(inputs.get(i));
                            book.setTitle(inputs.get(i+1));
                            book.setPress(inputs.get(i+2));
                            book.setPublishYear(Integer.parseInt(inputs.get(i+3)));
                            book.setAuthor(inputs.get(i+4));
                            book.setPrice(Double.parseDouble(inputs.get(i+5)));
                            book.setStock(Integer.parseInt(inputs.get(i+6)));
                            books.add(book);
                        }
                        ApiResult result = library.storeBook(books);
                        System.out.println(result.message);
                    } catch (IOException e) {
                        System.out.print("Exception");
                    }
                } else if (choice == 3) {
                    System.out.println("Please input the bookId and deltaStock separated by a white space: ");
                    String str = br.readLine();
                    StringTokenizer in = new StringTokenizer(str);
                    int bookId = Integer.parseInt(in.nextToken());
                    int deltaStock = Integer.parseInt(in.nextToken());
                    ApiResult rs = library.incBookStock(bookId,deltaStock);
                    System.out.println(rs.message);
                } else if (choice == 4) {
                    System.out.println("Please input the bookId: ");
                    int bookId = Integer.parseInt(br.readLine());
                    ApiResult rs = library.removeBook(bookId);
                    System.out.println(rs.message);
                } else if (choice == 5) {
                    System.out.println("Please input bookId and attributes in order(each attribute in a new line): bookId category title press publishYear author price stock");
                    int bookId = Integer.parseInt(br.readLine());
                    String category = br.readLine();
                    String title = br.readLine();
                    String press = br.readLine();
                    int publishYear = Integer.parseInt(br.readLine());
                    String author = br.readLine();
                    Double price = Double.parseDouble(br.readLine());
                    int stock = Integer.parseInt(br.readLine());
                    Book b = new Book(category, title, press, publishYear, author, price, stock);
                    b.setBookId(bookId);
                    ApiResult rs = library.modifyBookInfo(b);
                    System.out.println(rs.message);
                } else if (choice == 6) {
                    System.out.println("Please input query conditions: attribute_index value:");
                    System.out.println("attribute_index : 1 category; 2 title; 3 press; 4 min_publishYear; 5 max_publishYear; 6 author; 7 min_price; 8 max_price; 9 sort by(column); 10 order(asc/desc)");
                    System.out.println("Sample:\n1\ncomputer science\n3\npress-0\n9\nPRICE\n10\nDESC");
                    System.out.println("Your query(enter 'end' to stop):");
                    BookQueryConditions queryCondition = new BookQueryConditions();
                    String str = br.readLine();
                    while(!str.contentEquals("end")){
                        int op = Integer.parseInt(str);
                        switch (op){
                            case 1: queryCondition.setCategory(br.readLine()); break;
                            case 2: queryCondition.setTitle(br.readLine()); break;
                            case 3: queryCondition.setPress(br.readLine()); break;
                            case 4: queryCondition.setMinPublishYear(Integer.parseInt(br.readLine())); break;
                            case 5: queryCondition.setMaxPublishYear(Integer.parseInt(br.readLine())); break;
                            case 6: queryCondition.setAuthor(br.readLine()); break;
                            case 7: queryCondition.setMinPrice(Double.parseDouble(br.readLine())); break;
                            case 8: queryCondition.setMaxPrice(Double.parseDouble(br.readLine())); break;
                            case 9: queryCondition.setSortBy(Book.SortColumn.valueOf(br.readLine())); break;
                            case 10: queryCondition.setSortOrder(SortOrder.valueOf(br.readLine())); break;
                            default: System.out.println("Error!"); break;
                        }
                        str = br.readLine();
                    }
                    ApiResult queryResult = library.queryBook(queryCondition);
                    System.out.println(queryResult.message);
                    if(queryResult.ok){
                        BookQueryResults bookResults = (BookQueryResults) queryResult.payload;
                        for (int i = 0; i < bookResults.getCount(); i++) {
                            Book b = bookResults.getResults().get(i);
                            System.out.println(b.toString());
                        }
                    }
                } else if (choice == 7 || choice == 8) {
                    System.out.println("Please input bookId and cardId(seperated by a white space):");
                    String str = br.readLine();
                    StringTokenizer in = new StringTokenizer(str);
                    int bookId = Integer.parseInt(in.nextToken());
                    int cardId = Integer.parseInt(in.nextToken());
                    Borrow b = new Borrow(bookId, cardId);
                    ApiResult rs = null;
                    Date date = new Date();
                    long stamp = date.getTime()/1000;
                    if(choice == 7){
                        b.resetBorrowTime();
                        rs = library.borrowBook(b);
                    }
                    else{
                        b.resetReturnTime();
                        rs = library.returnBook(b);
                    }
                    System.out.println(rs.message);
                } else if (choice == 9) {
                    System.out.println("Please input cardId:");
                    int cardId = Integer.parseInt(br.readLine());
                    ApiResult result = library.showBorrowHistory(cardId);
                    System.out.println(result.message);
                    if(result.ok){
                        BorrowHistories items = (BorrowHistories) result.payload;
                        for (int i = 0; i < items.getCount(); i++) {
                            BorrowHistories.Item b = items.getItems().get(i);
                            System.out.println(b.toString());
                        }
                    }
                } else if (choice == 10) {
                    System.out.println("Please input attributes in order(each attribute in a new line): name department type");
                    String name = br.readLine();
                    String department = br.readLine();
                    String type = br.readLine();
                    Card c = new Card(0, name, department, Card.CardType.values(type));
                    ApiResult rs = library.registerCard(c);
                    System.out.println(rs.message);
                } else if (choice == 11) {
                    System.out.println("Please input the cardId: ");
                    int cardId = Integer.parseInt(br.readLine());
                    ApiResult rs = library.removeCard(cardId);
                    System.out.println(rs.message);
                } else if (choice == 12) {
                    ApiResult result = library.showCards();
                    System.out.println(result.message);
                    if(result.ok){
                        CardList cl = (CardList) result.payload;
                        for (int i = 0; i < cl.getCount(); i++) {
                            Card c = cl.getCards().get(i);
                            System.out.println(c.toString());
                        }
                    }
                } else if (choice == 13) {
                    ApiResult rs = library.resetDatabase();
                    System.out.println(rs.message);
                }
                System.out.print("\nPlease choose a option by its serial number: ");
                choice = Integer.parseInt(br.readLine());
            }

            System.out.println("Bye~");
            // release database connection handler
            if (connector.release()) {
                log.info("Success to release connection.");
            } else {
                log.warning("Failed to release connection.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
