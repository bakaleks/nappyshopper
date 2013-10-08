import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;
import java.util.Enumeration;
import javax.swing.*;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class NappyShopper {
    Keyspace keyspace;
    private AstyanaxContext<Keyspace> context;

    Date cartCreated;
    String[] keyspaces = new String[]{"meetup2_rf1", "meetup2_rf2", "meetup2_rf3", "meetup2_rf5", "meetup2_rf3_dc"};

    public NappyShopper() {
        usernameField.setText("user" + Math.round(Math.random()*100000));
        numCartsField.setText("1000");
        keyspacesList.setSelectedIndex(0);
        createCarts();

        fillLibero.addActionListener(new ActionListener() {
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
        fillPampers.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

            }
        });
        fillPampers.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                fill_carts("Pampers", "70 kroner");
            }
        });
        checkoutButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                errorsArea.setText("");
                outputArea.setText("");
                String readConsistency = getSelectedButtonText(readConsistencyButtonGroup);
                try {
                    Keyspace keyspace = getKeyspace();
                    String username = usernameField.getText();
                    long cartCreatedAt = cartCreated.getTime();
                    int numberOfCarts = Integer.parseInt(numCartsField.getText());
                    ColumnFamily<String, String> CF = ColumnFamily.newColumnFamily(
                            "shoppingcarts",
                            StringSerializer.get(),
                            StringSerializer.get());

                    int exceptionCounter = 0;
                    int numCartsWithNothing = 0;
                    int numCartsWithPampers = 0;
                    int numCartsWithLibero = 0;
                    int numCartsWithBoth = 0;

                    for (int cartNumber = 1; cartNumber <= numberOfCarts; cartNumber++) {
                        try {
                            String rowKey = username + ":" + cartCreatedAt + ":" + cartNumber;
                            outputArea.append("Read row " + rowKey + "\n");
                            ColumnList<String> result = keyspace.prepareQuery(CF)
                                    .setConsistencyLevel(ConsistencyLevel.valueOf("CL_" + readConsistency))
                                    .getKey(rowKey)
                                    .execute().getResult();
                            if (result.size() == 2) {
                                numCartsWithBoth++;
                            } else if (result.isEmpty()) {
                                numCartsWithNothing ++;
                            } else {
                                for (Column<String> column : result) {
                                    String shoppingItem = column.getName();
                                    if (shoppingItem.equals("Libero")) {
                                        numCartsWithLibero++;
                                    } else {
                                        numCartsWithPampers++;
                                    }
                                }
                            }
                        } catch (Exception e1) {
                            exceptionCounter++;
                            errorsArea.append(e1.getMessage() + "\n");
                        }

                    }

                    final Date timenow = new Date();

                    String message = String.format("%d:%d:%d Fetched %d carts:\n%d with nothing, %d with Libero, %d with Pampers, %d with both\n", timenow.getHours(), timenow.getMinutes(),
                            timenow.getSeconds(), numberOfCarts - exceptionCounter, numCartsWithNothing, numCartsWithLibero, numCartsWithPampers, numCartsWithBoth);
                    resultArea.setText(message);
                } catch (Exception exe) {
                    errorsArea.append(exe.getMessage() + "\n");
                }

            }
        });
    }

    private void fill_carts(String shopping_item, String price) {
        errorsArea.setText("");
        outputArea.setText("");
        String writeConsistency = getSelectedButtonText(writeConsistencyLevelsGroup);
        try {

            Keyspace keyspace = getKeyspace();
            ColumnFamily<String, String> CF = ColumnFamily.newColumnFamily(
                    "shoppingcarts",
                    StringSerializer.get(),
                    StringSerializer.get());

            String username = usernameField.getText();
            long cartCreatedAt = cartCreated.getTime();
            int numberOfCarts = Integer.parseInt(numCartsField.getText());

            int exceptionCounter = 0;
            for (int cartNumber = 1; cartNumber <= numberOfCarts; cartNumber++) {
                try {
                    String rowKey = username + ":" + cartCreatedAt + ":" + cartNumber;
                    outputArea.append("Writing row " + rowKey + "\n");
                    keyspace.prepareColumnMutation(CF, rowKey, shopping_item).setConsistencyLevel(
                            ConsistencyLevel.valueOf("CL_" + writeConsistency)).putValue(price, null).execute();
                } catch (Exception e) {
                    exceptionCounter++;
                    errorsArea.append(e.getMessage() + "\n");
                }
            }

            final Date timenow = new Date();

            String message = String.format("%d:%d:%d Added %s to %d carts. %d carts failed\n", timenow.getHours(), timenow.getMinutes(),
                    timenow.getSeconds(), shopping_item, numberOfCarts - exceptionCounter, exceptionCounter);
            resultArea.setText(message);
        } catch (Exception e) {
            errorsArea.append(e.getMessage() + "\n");
        }
    }

    private Keyspace getKeyspace() {
        String keyspaceName = keyspaces[keyspacesList.getSelectedIndex()];
        if (keyspace != null && keyspace.getKeyspaceName().equals(keyspaceName)) {
            return keyspace;
        }

        if (context != null) {
            context.shutdown();
        }

        context = new AstyanaxContext.Builder()
                .forCluster("TestCluster")
                .forKeyspace(keyspaceName)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                        .setCqlVersion("3.0.0")
                        .setTargetCassandraVersion("1.2")
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setSeeds("127.0.0.1:9160")
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();

        Keyspace keyspace = context.getClient();
        this.keyspace = keyspace;
        return keyspace;
    }


    private void createCarts() {
        cartCreated = new Date();
        cartCreatedField.setText(cartCreated.toString());
        resultArea.setText("");
        errorsArea.setText("");
        outputArea.setText("");
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
        JFrame frame = new JFrame("NappyShopper");
        frame.setContentPane(new NappyShopper().panel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    private JPanel panel1;
    private JTextArea errorsArea;
    private JButton fillLibero;
    private JButton checkoutButton;
    private JTextField usernameField;
    private JTextField numCartsField;
    private JButton createNewCartButton;
    private JLabel cartCreatedField;
    private JButton fillPampers;
    private JList keyspacesList;
    private JTextArea resultArea;
    private JTextArea outputArea;
    private ButtonGroup readConsistencyButtonGroup;
    private ButtonGroup writeConsistencyLevelsGroup;
}
