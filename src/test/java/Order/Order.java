package Order;


import lombok.Data;

@Data
public class Order {
    private String product;
    private String username;
    private String orderId;
    private Long orderTime;
}
