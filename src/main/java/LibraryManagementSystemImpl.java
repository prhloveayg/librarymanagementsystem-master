import com.mysql.cj.protocol.Resultset;
import entities.Book;
import entities.Borrow;
import entities.Card;
import queries.*;
import utils.DBInitializer;
import utils.DatabaseConnector;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.time.Instant;

public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
    public ApiResult storeBook(Book book) {
        Connection conn = connector.getConn();
        try{
            String sql = "insert into book(category,title,press,publish_year,author,price,stock) values(?,?,?,?,?,?,?)";
            PreparedStatement pst = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setString(1, book.getCategory());
            pst.setString(2, book.getTitle());
            pst.setString(3, book.getPress());
            pst.setInt(4, book.getPublishYear());
            pst.setString(5, book.getAuthor());
            pst.setBigDecimal(6, new BigDecimal(book.getPrice()));
            pst.setInt(7, book.getStock());
            int rowCount = pst.executeUpdate();
            if(rowCount > 0) {
                commit(conn);
                ResultSet rs = pst.getGeneratedKeys();
                int id;
                if(rs.next()){
                    id = rs.getInt(1);
                    book.setBookId(id);
                }
            }
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, "Success to add the book!");
    }

    @Override
    public ApiResult incBookStock(int bookId, int deltaStock) {
        Connection conn = connector.getConn();
        try{
            String sql = "select stock from book where book_id = ?";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setInt(1, bookId);
            ResultSet rs = pst.executeQuery();
            if(rs.next()) {
                try {
                    int stock = rs.getInt(1);
                    int final_stock = stock + deltaStock;
                    if (final_stock >= 0) {
                        sql = "update book set stock = ? where book_id = ?";
                        pst = conn.prepareStatement(sql);
                        pst.setInt(1, final_stock);
                        pst.setInt(2, bookId);
                        int rowCount = pst.executeUpdate();
                        if(rowCount > 0){
                            commit(conn);
                            return new ApiResult(true, "Success to change the book stock!");
                        }
                    }
                } catch (SQLException e) {
                    rollback(conn);
                    return new ApiResult(false, e.getMessage());
                }
            }
            commit(conn);
            return new ApiResult(false, "Error!");
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult storeBook(List<Book> books) {
        Connection conn = connector.getConn();
        for(Book book : books){
            try{
                String sql = "insert into book(category,title,press,publish_year,author,price,stock) values(?,?,?,?,?,?,?)";
                PreparedStatement pst = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pst.setString(1, book.getCategory());
                pst.setString(2, book.getTitle());
                pst.setString(3, book.getPress());
                pst.setInt(4, book.getPublishYear());
                pst.setString(5, book.getAuthor());
                pst.setBigDecimal(6, new BigDecimal(book.getPrice()));
                pst.setInt(7, book.getStock());
                int rowCount = pst.executeUpdate();
                if(rowCount > 0) {
                    ResultSet rs = pst.getGeneratedKeys();
                    int id;
                    if(rs.next()){
                        id = rs.getInt(1);
                        book.setBookId(id);
                    }
                }
            }catch(SQLException e) {
                rollback(conn);
                return new ApiResult(false, e.getMessage());
            }
        }
        commit(conn);
        return new ApiResult(true, "Success to store books!");
    }

    @Override
    public ApiResult removeBook(int bookId) {
        Connection conn = connector.getConn();
        try{
            String sql = "select * from borrow where book_id = ? and return_time = 0";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setInt(1,bookId);
            ResultSet rs = pst.executeQuery();
            if(rs.next()){
                return new ApiResult(false, "Error!");
            }
            sql = "delete from book where book_id = ?";
            pst = conn.prepareStatement(sql);
            pst.setInt(1,bookId);
            int rowCount = pst.executeUpdate();
            if(rowCount > 0){
                return new ApiResult(true, "Success to remove the book!");
            }
            return new ApiResult(false, "Error!");
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult modifyBookInfo(Book book) {
        Connection conn = connector.getConn();
        try{
            String sql = "update book set category = ?,title = ?,press = ?,publish_year = ?,author = ?,price = ? where book_id = ?";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setString(1, book.getCategory());
            pst.setString(2, book.getTitle());
            pst.setString(3, book.getPress());
            pst.setInt(4, book.getPublishYear());
            pst.setString(5, book.getAuthor());
            pst.setBigDecimal(6, new BigDecimal(book.getPrice()));
            pst.setInt(7, book.getBookId());
            int rowCount = pst.executeUpdate();
            if(rowCount > 0) {
                commit(conn);
                return new ApiResult(true, "Success to modify book info!");
            }
            return new ApiResult(false, "Error!");
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult queryBook(BookQueryConditions conditions) {
        Connection conn = connector.getConn();
        String category = conditions.getCategory();
        String title = conditions.getTitle();
        String press = conditions.getPress();
        Double max_price = conditions.getMaxPrice();
        Double min_price = conditions.getMinPrice();
        String author = conditions.getAuthor();
        Book.SortColumn sort_by = conditions.getSortBy();
        SortOrder sort_order = conditions.getSortOrder();
        String regexp = "\'";
        String str1 = sort_by.getValue();
        str1 = str1.replaceAll(regexp, "");
        String str2 = sort_order.getValue();
        str2 = str2.replaceAll(regexp, "");
        PreparedStatement pst;
        String sql;
        ResultSet rs1 = null;
        ResultSet rs2 = null;
        ResultSet rs3 = null;
        ResultSet rs4 = null;
        ResultSet rs5 = null;
        ResultSet rs6 = null;
        ResultSet rs7 = null;
        try {
            if(str1 != "book_id"){
                sql = "select * from book order by " + str1 + " " + str2 + ", book_id asc";
            }
            else{
                sql = "select * from book order by " + str1 + " " + str2;
            }
            pst = conn.prepareStatement(sql);
            rs1 = pst.executeQuery();
            List<Book> books = new ArrayList<>();
            while(rs1.next()) {
                Book book = new Book();
                book.setBookId(rs1.getInt("book_id"));
                book.setCategory(rs1.getString("category"));
                book.setTitle(rs1.getString("title"));
                book.setPress(rs1.getString("press"));
                book.setPublishYear(rs1.getInt("publish_year"));
                book.setAuthor(rs1.getString("author"));
                book.setPrice(rs1.getDouble("price"));
                book.setStock(rs1.getInt("stock"));
                books.add(book);
            }
            if(category!=null){
                if(str1 != "book_id"){
                    sql = "select * from book where category = ? order by " + str1 + " " + str2 + ", book_id asc";
                }
                else{
                    sql = "select * from book where category = ? order by " + str1 + " " + str2;
                }
                pst = conn.prepareStatement(sql);
                pst.setString(1, category);
                rs2 = pst.executeQuery();
                List<Book> books1 = new ArrayList<>();
                while(rs2.next()) {
                    Book book = new Book();
                    book.setBookId(rs2.getInt("book_id"));
                    book.setCategory(rs2.getString("category"));
                    book.setTitle(rs2.getString("title"));
                    book.setPress(rs2.getString("press"));
                    book.setPublishYear(rs2.getInt("publish_year"));
                    book.setAuthor(rs2.getString("author"));
                    book.setPrice(rs2.getDouble("price"));
                    book.setStock(rs2.getInt("stock"));
                    books1.add(book);
                }
                books.retainAll(books1);
            }
            if(title!=null){
                if(str1 != "book_id"){
                    sql = "select * from book where title like concat('%',?,'%') order by " + str1 + " " + str2 + ", book_id asc";
                }
                else{
                    sql = "select * from book where title like concat('%',?,'%') order by " + str1 + " " + str2;
                }
                pst = conn.prepareStatement(sql);
                pst.setString(1, title);
                rs3 = pst.executeQuery();
                List<Book> books2 = new ArrayList<>();
                while(rs3.next()) {
                    Book book = new Book();
                    book.setBookId(rs3.getInt("book_id"));
                    book.setCategory(rs3.getString("category"));
                    book.setTitle(rs3.getString("title"));
                    book.setPress(rs3.getString("press"));
                    book.setPublishYear(rs3.getInt("publish_year"));
                    book.setAuthor(rs3.getString("author"));
                    book.setPrice(rs3.getDouble("price"));
                    book.setStock(rs3.getInt("stock"));
                    books2.add(book);
                }
                books.retainAll(books2);
            }
            if(press!=null){
                if(str1 != "book_id"){
                    sql = "select * from book where press like concat('%',?,'%') order by " + str1 + " " + str2 + ", book_id asc";
                }
                else{
                    sql = "select * from book where press like concat('%',?,'%') order by " + str1 + " " + str2;
                }
                pst = conn.prepareStatement(sql);
                pst.setString(1, press);
                rs4 = pst.executeQuery();
                List<Book> books3 = new ArrayList<>();
                while(rs4.next()) {
                    Book book = new Book();
                    book.setBookId(rs4.getInt("book_id"));
                    book.setCategory(rs4.getString("category"));
                    book.setTitle(rs4.getString("title"));
                    book.setPress(rs4.getString("press"));
                    book.setPublishYear(rs4.getInt("publish_year"));
                    book.setAuthor(rs4.getString("author"));
                    book.setPrice(rs4.getDouble("price"));
                    book.setStock(rs4.getInt("stock"));
                    books3.add(book);
                }
                books.retainAll(books3);
            }
            if(author!=null){
                if(str1 != "book_id"){
                    sql = "select * from book where author like concat('%',?,'%') order by " + str1 + " " + str2 + ", book_id asc";
                }
                else{
                    sql = "select * from book where author like concat('%',?,'%') order by " + str1 + " " + str2;
                }
                pst = conn.prepareStatement(sql);
                pst.setString(1, author);
                rs5 = pst.executeQuery();
                List<Book> books4 = new ArrayList<>();
                while(rs5.next()) {
                    Book book = new Book();
                    book.setBookId(rs5.getInt("book_id"));
                    book.setCategory(rs5.getString("category"));
                    book.setTitle(rs5.getString("title"));
                    book.setPress(rs5.getString("press"));
                    book.setPublishYear(rs5.getInt("publish_year"));
                    book.setAuthor(rs5.getString("author"));
                    book.setPrice(rs5.getDouble("price"));
                    book.setStock(rs5.getInt("stock"));
                    books4.add(book);
                }
                books.retainAll(books4);
            }
            if(min_price!=null || max_price!=null){
                if(min_price!=null && max_price!=null){
                    if(str1 != "book_id"){
                        sql = "select * from book where price between ? and ? order by " + str1 + " " + str2 + ", book_id asc";
                    }
                    else{
                        sql = "select * from book where price between ? and ? order by " + str1 + " " + str2;
                    }
                    pst = conn.prepareStatement(sql);
                    pst.setDouble(1, min_price);
                    pst.setDouble(2, max_price);
                    rs6 = pst.executeQuery();
                }
                else if(min_price!=null && max_price==null){
                    if(str1 != "book_id"){
                        sql = "select * from book where price >= ? order by " + str1 + " " + str2 + ", book_id asc";
                    }
                    else{
                        sql = "select * from book where price >= ? order by " + str1 + " " + str2;
                    }
                    pst = conn.prepareStatement(sql);
                    pst.setDouble(1, min_price);
                    rs6 = pst.executeQuery();
                }
                else if(min_price==null && max_price!=null){
                    if(str1 != "book_id"){
                        sql = "select * from book where price <= ? order by " + str1 + " " + str2 + ", book_id asc";
                    }
                    else{
                        sql = "select * from book where price <= ? order by " + str1 + " " + str2;
                    }
                    pst = conn.prepareStatement(sql);
                    pst.setDouble(1, max_price);
                    rs6 = pst.executeQuery();
                }
                List<Book> books5 = new ArrayList<>();
                while(rs6.next()) {
                    Book book = new Book();
                    book.setBookId(rs6.getInt("book_id"));
                    book.setCategory(rs6.getString("category"));
                    book.setTitle(rs6.getString("title"));
                    book.setPress(rs6.getString("press"));
                    book.setPublishYear(rs6.getInt("publish_year"));
                    book.setAuthor(rs6.getString("author"));
                    book.setPrice(rs6.getDouble("price"));
                    book.setStock(rs6.getInt("stock"));
                    books5.add(book);
                }
                books.retainAll(books5);
            }
            if(conditions.getMaxPublishYear()!=null || conditions.getMinPublishYear()!=null){
                if(conditions.getMaxPublishYear()!=null && conditions.getMinPublishYear()!=null){
                    int max_publish_year = conditions.getMaxPublishYear();
                    int min_publish_year = conditions.getMinPublishYear();
                    if(str1 != "book_id"){
                        sql = "select * from book where publish_year between ? and ? order by " + str1 + " " + str2 + ", book_id asc";
                    }
                    else{
                        sql = "select * from book where publish_year between ? and ? order by " + str1 + " " + str2;
                    }
                    pst = conn.prepareStatement(sql);
                    pst.setDouble(1, min_publish_year);
                    pst.setDouble(2, max_publish_year);
                    rs7 = pst.executeQuery();
                }
                else if(conditions.getMaxPublishYear()!=null && conditions.getMinPublishYear()==null){
                    int max_publish_year = conditions.getMaxPublishYear();
                    if(str1 != "book_id"){
                        sql = "select * from book where publish_year <= ? order by " + str1 + " " + str2 + ", book_id asc";
                    }
                    else{
                        sql = "select * from book where publish_year <= ? order by " + str1 + " " + str2;
                    }
                    pst = conn.prepareStatement(sql);
                    pst.setDouble(1, max_publish_year);
                    rs7 = pst.executeQuery();
                }
                else if(conditions.getMaxPublishYear()==null && conditions.getMinPublishYear()!=null){
                    int min_publish_year = conditions.getMinPublishYear();
                    if(str1 != "book_id"){
                        sql = "select * from book where publish_year >= ? order by " + str1 + " " + str2 + ", book_id asc";
                    }
                    else{
                        sql = "select * from book where publish_year >= ? order by " + str1 + " " + str2;
                    }
                    pst = conn.prepareStatement(sql);
                    pst.setDouble(1, min_publish_year);
                    rs7 = pst.executeQuery();
                }
                List<Book> books6 = new ArrayList<>();
                while(rs7.next()) {
                    Book book = new Book();
                    book.setBookId(rs7.getInt("book_id"));
                    book.setCategory(rs7.getString("category"));
                    book.setTitle(rs7.getString("title"));
                    book.setPress(rs7.getString("press"));
                    book.setPublishYear(rs7.getInt("publish_year"));
                    book.setAuthor(rs7.getString("author"));
                    book.setPrice(rs7.getDouble("price"));
                    book.setStock(rs7.getInt("stock"));
                    books6.add(book);
                }
                books.retainAll(books6);
            }
            BookQueryResults result = new BookQueryResults(books);
            ApiResult r = new ApiResult(true, "The query result:", result);
            commit(conn);
            return r;
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult borrowBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try{
            String sql = "select stock from book where book_id = ? lock in share mode";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setInt(1, borrow.getBookId());
            ResultSet rs = pst.executeQuery();
            if(rs.next()) {
                 if(rs.getInt(1) > 0){
                     try{
                         sql = "select return_time from borrow where card_id = ? and book_id = ?";
                         pst = conn.prepareStatement(sql);
                         pst.setInt(1, borrow.getCardId());
                         pst.setInt(2, borrow.getBookId());
                         rs = pst.executeQuery();
                         while(rs.next()){
                             if(rs.getLong(1) == 0){
                                 return new ApiResult(false, "Error!");
                             }
                             if(rs.getLong(1) >= borrow.getBorrowTime()){
                                 return new ApiResult(false, "Error!");
                             }
                         }
                     }catch(SQLException e) {
                         rollback(conn);
                         return new ApiResult(false, e.getMessage());
                     }
                 }else{
                     return new ApiResult(false, "Error!");
                 }
            }else{
                return new ApiResult(false, "Error!");
            }
            try{
                sql = "insert into borrow(card_id,book_id,borrow_time,return_time) values(?,?,?,0)";
                pst = conn.prepareStatement(sql);
                pst.setInt(1, borrow.getCardId());
                pst.setInt(2, borrow.getBookId());
                pst.setLong(3, borrow.getBorrowTime());
                int rowCount = pst.executeUpdate();
                if(rowCount > 0) {
                    try{
                        sql = "update book set stock = stock - 1 where book_id = ?";
                        pst = conn.prepareStatement(sql);
                        pst.setInt(1, borrow.getBookId());
                        rowCount = pst.executeUpdate();
                        if(rowCount > 0){
                            /*sql = "select stock from book where book_id = ?";
                            pst = conn.prepareStatement(sql);
                            pst.setInt(1, borrow.getBookId());
                            rs = pst.executeQuery();
                            if(rs.next()){
                                if(rs.getInt(1) < 0){
                                    rollback(conn);
                                    return new ApiResult(false, "Error!");
                                }
                            }*/
                            commit(conn);
                            return new ApiResult(true, "Success to borrow the book!");
                        }
                    }catch(SQLException e) {
                        rollback(conn);
                        return new ApiResult(false, e.getMessage());
                    }
                }
            }catch(SQLException e) {
                rollback(conn);
                return new ApiResult(false, e.getMessage());
            }
            return new ApiResult(false, "Error!");
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try{
            String sql = "select borrow_time from borrow where book_id = ? and card_id = ? and return_time = 0";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setInt(1, borrow.getBookId());
            pst.setInt(2, borrow.getCardId());
            ResultSet rs = pst.executeQuery();
            long b_time, r_time;
            if(rs.next()) {
                b_time = rs.getLong(1);
                if(b_time >= borrow.getReturnTime()){
                    return new ApiResult(false, "Error!");
                }
            }
            sql = "update borrow set return_time = ? where book_id = ? and card_id = ? and return_time = 0";
            pst = conn.prepareStatement(sql);
            pst.setLong(1, borrow.getReturnTime());
            pst.setInt(2,borrow.getBookId());
            pst.setInt(3,borrow.getCardId());
            int rowCount = pst.executeUpdate();
            if(rowCount > 0){
                try{
                    sql = "update book set stock = stock + 1 where book_id = ?";
                    pst = conn.prepareStatement(sql);
                    pst.setInt(1, borrow.getBookId());
                    rowCount = pst.executeUpdate();
                    if(rowCount > 0){
                        commit(conn);
                        return new ApiResult(true, "Success to return the book!");
                    }
                }catch(SQLException e) {
                    rollback(conn);
                    return new ApiResult(false, e.getMessage());
                }
            }
            return new ApiResult(false, "Error!");
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult showBorrowHistory(int cardId) {
        Connection conn = connector.getConn();
        try{
            String sql = "select card_id,book.book_id,category,title,press,publish_year,author,price,borrow_time,return_time from borrow,book where borrow.book_id = book.book_id and card_id = ? order by borrow_time DESC, book_id ASC";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setInt(1, cardId);
            ResultSet rs = pst.executeQuery();
            List<BorrowHistories.Item> items = new ArrayList<>();
            while(rs.next()) {
                Borrow borrow = new Borrow();
                Book book = new Book();
                int cardID = rs.getInt(1);
                book.setBookId(rs.getInt(2));
                book.setCategory(rs.getString(3));
                book.setTitle(rs.getString(4));
                book.setPress(rs.getString(5));
                book.setPublishYear(rs.getInt(6));
                book.setAuthor(rs.getString(7));
                book.setPrice(rs.getDouble(8));
                borrow.setBorrowTime(rs.getLong(9));
                borrow.setReturnTime(rs.getLong(10));
                BorrowHistories.Item item = new BorrowHistories.Item(cardId, book, borrow);
                items.add(item);
            }
            BorrowHistories result = new BorrowHistories(items);
            ApiResult r = new ApiResult(true, "Borrow History:", result);
            commit(conn);
            return r;
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult registerCard(Card card) {
        Connection conn = connector.getConn();
        try{
            String sql = "insert into card(name,department,type) values(?,?,?)";
            PreparedStatement pst = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pst.setString(1, card.getName());
            pst.setString(2, card.getDepartment());
            pst.setString(3, card.getType().getStr());
            int rowCount = pst.executeUpdate();
            if(rowCount > 0) {
                commit(conn);
                ResultSet rs = pst.getGeneratedKeys();
                int id;
                if(rs.next()){
                    id = rs.getInt(1);
                    card.setCardId(id);
                }
            }
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, "Success to add the card!");
    }

    @Override
    public ApiResult removeCard(int cardId) {
        Connection conn = connector.getConn();
        try{
            String sql = "select * from borrow where card_id = ? and return_time = 0";
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setInt(1,cardId);
            ResultSet rs = pst.executeQuery();
            if(rs.next()){
                return new ApiResult(false, "Error!");
            }
            sql = "delete from card where card_id = ?";
            pst = conn.prepareStatement(sql);
            pst.setInt(1,cardId);
            int rowCount = pst.executeUpdate();
            if(rowCount > 0){
                return new ApiResult(true, "Success to remove the card!");
            }
            return new ApiResult(false, "Error!");
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }

    }

    @Override
    public ApiResult showCards() {
        Connection conn = connector.getConn();
        try{
            String sql = "select * from card order by card_id";
            PreparedStatement pst = conn.prepareStatement(sql);
            ResultSet rs = pst.executeQuery();
            List<Card> cards = new ArrayList<>();
            while(rs.next()){
                Card card = new Card();
                card.setCardId(rs.getInt("card_id"));
                card.setName(rs.getString("name"));
                card.setDepartment(rs.getString("department"));
                card.setType(Card.CardType.values(rs.getString("type")));
                cards.add(card);
            }
            CardList result = new CardList(cards);
            return new ApiResult(true, "The cards:", result);
        }catch(SQLException e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult resetDatabase() {
        Connection conn = connector.getConn();
        try {
            Statement stmt = conn.createStatement();
            DBInitializer initializer = connector.getConf().getType().getDbInitializer();
            stmt.addBatch(initializer.sqlDropBorrow());
            stmt.addBatch(initializer.sqlDropBook());
            stmt.addBatch(initializer.sqlDropCard());
            stmt.addBatch(initializer.sqlCreateCard());
            stmt.addBatch(initializer.sqlCreateBook());
            stmt.addBatch(initializer.sqlCreateBorrow());
            stmt.executeBatch();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, "Success to reset database!");
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void commit(Connection conn) {
        try {
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
