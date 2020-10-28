/**
 * @author Snowson
 * @date 2020/10/16 17:59
 */
public class Transaction {
    private String accountId;
    private Integer value;

    public Transaction(String accountId, Integer value) {
        this.accountId = accountId;
        this.value = value;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "accountId='" + accountId + '\'' +
                ", value=" + value +
                '}';
    }
}
