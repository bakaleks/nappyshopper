import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import javax.swing.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class ConsistencyLab {
    private JRadioButton ANYRadioButton;
    private JPanel panel1;
    private JRadioButton ONERadioButton;

    String[] keyspaces = new String[]{"meetup2_1", "meetup2_2", "meetup2_3"};

    Date cartCreated;
    private void fill_carts(String shopping_item, String price) {
        outputArea.setText("");
        cqlArea.setText("");
        String writeConsistency = getSelectedButtonText(writeConsistencyLevelsGroup);
        try {
            Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
            Session session = cluster.connect();

            String keyspace = keyspaces[keyspacesList.getSelectedIndex()];
            String username = usernameField.getText();
            long cartCreatedAt = cartCreated.getTime();
            int numberOfCarts = Integer.parseInt(numCartsField.getText());

            int exceptionCounter = 0;
            List<ResultSetFuture> resultSetFutureList = new ArrayList<ResultSetFuture>();
            for (int cartNumber = 1; cartNumber <= numberOfCarts; cartNumber++) {
                //CREATE TABLE shoppingcarts ( username text, creation_time bigint, cart_number int, shopping_item text, price text, PRIMARY KEY ((username, creation_time, cart_number), shopping_item));
                final Query insertQuery = QueryBuilder
                        .insertInto(keyspace, "shoppingcarts")
                        .values(new String[]{"username", "creation_time", "cart_number", "shopping_item", "price"},
                                new Object[]{username, cartCreatedAt, cartNumber, shopping_item, price})
                        .setConsistencyLevel(ConsistencyLevel.valueOf(writeConsistency));
                cqlArea.append(insertQuery.toString() + "\n\n");
                ResultSetFuture resultSetFuture = session.executeAsync(insertQuery);
                resultSetFutureList.add(resultSetFuture);
            }

            for (ResultSetFuture resultSetFuture : resultSetFutureList) {
                try {
                    resultSetFuture.getUninterruptibly();
                } catch (Exception e) {
                    exceptionCounter++;
                    outputArea.append(e.getMessage() + "\n");
                }
            }
            final Date timenow = new Date();

            String message = String.format("%d:%d:%d Added %s to %d carts. %d carts failed\n", timenow.getHours(), timenow.getMinutes(),
                    timenow.getSeconds(), shopping_item, numberOfCarts - exceptionCounter, exceptionCounter);
            counterArea.setText(message);
            cluster.shutdown();
        } catch (Exception e) {
            outputArea.append(e.getMessage() + "\n");
        }
    }

    public ConsistencyLab() {
        usernameField.setText("user" + Math.round(Math.random()*100000));
        numCartsField.setText("1000");
        keyspacesList.setSelectedIndex(0);
        createCarts();

        fill50Button.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                fill_carts("Libero", "60 kroner");

            }
        });


        createNewCartButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                createCarts();
            }
        });
        fill100Button.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

            }
        });
        fill100Button.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                fill_carts("Pampers", "70 kroner");
            }
        });
        checkoutButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                outputArea.setText("");
                cqlArea.setText("");
                String readConsistency = getSelectedButtonText(readConsistencyButtonGroup);
                try {
                    Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
                    Session session = cluster.connect();

                    String keyspace = keyspaces[keyspacesList.getSelectedIndex()];
                    String username = usernameField.getText();
                    long cartCreatedAt = cartCreated.getTime();
                    int numberOfCarts = Integer.parseInt(numCartsField.getText());

                    int exceptionCounter = 0;
                    int numCartsZeroItems = 0;
                    int numCartsOneItem = 0;
                    int numCartsTwoItems = 0;
                    List<ResultSetFuture> resultSetFutureList = new ArrayList<ResultSetFuture>();

                    for (int cartNumber = 1; cartNumber <= numberOfCarts; cartNumber++) {
                        //CREATE TABLE shoppingcarts ( username text, creation_time bigint, cart_number int, shopping_item text, price text, PRIMARY KEY ((username, creation_time, cart_number), shopping_item));

                        final Query checkoutQuery = QueryBuilder.select().countAll().from(keyspace, "shoppingcarts")
                                .where(QueryBuilder.eq("username", username))
                                .and(QueryBuilder.eq("creation_time", cartCreatedAt))
                                .and(QueryBuilder.eq("cart_number", cartNumber)).setConsistencyLevel(
                                        ConsistencyLevel.valueOf(readConsistency));
                        cqlArea.append(checkoutQuery.toString() + "\n\n");
                        ResultSetFuture resultFuture = session.executeAsync(checkoutQuery);

                        resultSetFutureList.add(resultFuture);
                    }

                    for (ResultSetFuture resultSetFuture : resultSetFutureList) {
                        try {
                            ResultSet result = resultSetFuture.getUninterruptibly();
                            long itemsCount = result.one().getLong("count");
                            if (itemsCount == 1) {
                                numCartsOneItem++;
                            } else if (itemsCount == 2) {
                                numCartsTwoItems++;
                            } else {
                                numCartsZeroItems++;
                            }
                        } catch (Exception e1) {
                            exceptionCounter++;
                            outputArea.append(e1.getMessage() + "\n");
                        }
                    }


                    final Date timenow = new Date();

                    String message = String.format("%d:%d:%d Fetched %d carts:\n%d with no nappies, %d with one nappy type, %d with two nappy types\n", timenow.getHours(), timenow.getMinutes(),
                            timenow.getSeconds(), numberOfCarts - exceptionCounter, numCartsZeroItems, numCartsOneItem, numCartsTwoItems);
                    counterArea.setText(message);
                    cluster.shutdown();
                } catch (Exception exe) {
                    outputArea.append(exe.getMessage() + "\n");
                }

            }
        });
    }


    private void createCarts() {
        cartCreated = new Date();
        cartCreatedField.setText(cartCreated.toString());
        counterArea.setText("");
        outputArea.setText("");
        cqlArea.setText("");
    }

    public String getSelectedButtonText(ButtonGroup buttonGroup) {
        for (Enumeration<AbstractButton> buttons = buttonGroup.getElements(); buttons.hasMoreElements();) {
            AbstractButton button = buttons.nextElement();

            if (button.isSelected()) {
                return button.getText();
            }
        }

        return null;
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("ConsistencyLab");
        frame.setContentPane(new ConsistencyLab().panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    private JRadioButton TWORadioButton;
    private JRadioButton THREERadioButton;
    private JRadioButton QUORUMRadioButton;
    private JRadioButton EACH_QUORUMRadioButton;
    private JRadioButton LOCAL_QUORUMRadioButton;
    private JRadioButton TWORadioButton1;
    private JRadioButton THREERadioButton1;
    private JRadioButton ONERadioButton1;
    private JRadioButton LOCAL_QUORUMRadioButton1;
    private JRadioButton EACH_QUORUMRadioButton1;
    private JRadioButton QUORUMRadioButton1;
    private JTextArea outputArea;
    private JButton fill50Button;
    private JButton checkoutButton;
    private JTextField usernameField;
    private JTextField numCartsField;
    private JButton createNewCartButton;
    private JLabel cartCreatedField;
    private JButton fill100Button;
    private JList keyspacesList;
    private JTextArea counterArea;
    private JTextArea cqlArea;
    private ButtonGroup readConsistencyButtonGroup;
    private ButtonGroup writeConsistencyLevelsGroup;

    public void setData(ConsistencyLabModel data) {
        usernameField.setText(data.getUsername());
        numCartsField.setText(data.getNumCarts());
        outputArea.setText(data.getOutput());
    }

    public void getData(ConsistencyLabModel data) {
        data.setUsername(usernameField.getText());
        data.setNumCarts(numCartsField.getText());
        data.setOutput(outputArea.getText());
    }

    public boolean isModified(ConsistencyLabModel data) {
        if (usernameField.getText() != null ? !usernameField.getText().equals(data.getUsername()) : data.getUsername() != null) {
            return true;
        }
        if (numCartsField.getText() != null ? !numCartsField.getText().equals(data.getNumCarts()) : data.getNumCarts() != null) {
            return true;
        }
        if (outputArea.getText() != null ? !outputArea.getText().equals(data.getOutput()) : data.getOutput() != null) {
            return true;
        }
        return false;
    }
}
