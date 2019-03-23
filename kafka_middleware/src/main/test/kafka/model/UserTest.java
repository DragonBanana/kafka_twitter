package kafka.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class UserTest {

    @Test
    public void equals() {
        User user1 = new User("luca");
        User user2 = new User("luca");

        assertEquals(user1, user2);
    }
}