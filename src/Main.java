import net.datafaker.Faker;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;


public class Main {

    public static void main(String[] args) {
        Connection connection = null;
        Faker faker = new Faker();
        try {
            // Load the PostgreSQL JDBC driver
            Class.forName("org.postgresql.Driver");

            // Establish a connection
            String url = "jdbc:postgresql://localhost:5432/randstad";
            String username = "randstadadmin";
            String password = "pass";
            connection = DriverManager.getConnection(url, username, password);

            // Check if the connection is successful
            if (connection != null) {
                System.out.println("Connection successful.");

                // Start a transaction
                connection.setAutoCommit(false);

                // DELETE EVERYTHING
                // deleteRecords("searchnmatch", "t_file", connection);
                // deleteRecords("searchnmatch", "t_cv_detail", connection);
                // deleteRecords("searchnmatch", "t_jd_detail", connection);
                //deleteRecords("searchnmatch", "t_cv_skills", connection);


                for (int i = 0; i < 1; i++) {
                    // INSERT DATA

                    feedCV(connection, faker);
                    feedJD(connection, faker);
                    feedSkills(connection);
                    feedSkillsEmbedding(connection);
                    feedCustomMatchData(connection, faker);

                    // System.out.println(">>>>>>>>>>>>>>>>>>>INSERTED " + (i + 1) + " ROWS<<<<<<<<<<<<<<<<<<");
                }
                feedskillsMapping(connection);

                modifyExistingCVTable(connection);
                // Commit the transaction
                connection.commit();
            } else {
                System.out.println("Connection failed.");
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            // Rollback the transaction in case of an exception
            if (connection != null) {
                try {
                    connection.rollback();
                    System.out.println("Transaction rolled back.");
                } catch (SQLException rollbackException) {
                    rollbackException.printStackTrace();
                }
            }
        } finally {
            // Close the connection in the finally block to ensure it's always closed
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                    System.out.println("Connection closed.");
                }
            } catch (SQLException closeException) {
                closeException.printStackTrace();
            }
        }
    }

    public static void feedCV(Connection connection, Faker faker) throws SQLException {
        // Insert into t_file table
        String fileQuery = "INSERT INTO searchnmatch.t_file (file_id, s3_object_key, file_status, file_name, " +
                "file_type, uploaded_by, created_datetime) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        UUID uuid = UUID.randomUUID();
        String fileId = uuid.toString();
        String name = faker.name().firstName();
        String email = faker.internet().emailAddress();

        PreparedStatement fileStatement = connection.prepareStatement(fileQuery);
        fileStatement.setString(1, fileId);
        fileStatement.setString(2, "internal/cv/" + name + ".pdf");
        fileStatement.setObject(3, getRandomFileStatus(), Types.OTHER);
        fileStatement.setString(4, name + ".pdf");
        fileStatement.setObject(5, Main.FILE_TYPE.CV, Types.OTHER);
        fileStatement.setString(6, email);
        fileStatement.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
        fileStatement.executeUpdate();


        // Insert into t_cv_detail table
        String cvQuery = "INSERT INTO searchnmatch.t_cv_detail (cv_id, cv_status, title, file_id, location, " +
                "experience, domain_name, name, email, phone, " +
                "summary, min_salary, max_salary, currency, company, team, languages, job_type, skills, " +
                "education, certifications, created_datetime) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        UUID uuid1 = UUID.randomUUID();
        String cvId = uuid1.toString();
        String[] languages = getRandomLanguages(new Random().nextInt(5) + 1);
        Array languagesArray = connection.createArrayOf("VARCHAR", languages);
        String randomTeam = getRandomTeam();

        String randomJobType = getRandomJobType();
        String[] skills = getRandomSkills(faker, new Random().nextInt(30));
        Array skillsArray = connection.createArrayOf("VARCHAR", skills);
        String[] educations = getRandomEducations(faker, new Random().nextInt(10));
        Array educationsArray = connection.createArrayOf("VARCHAR", educations);
        String minSalary = roundToNearestHundred(faker.number().numberBetween(10000, 20000)) + "";
        String maxSalary = roundToNearestHundred(faker.number().numberBetween(20000, 1000000)) + "";
        String experience = faker.number().numberBetween(10, 200) + "";
        String[] certifications = getRandomCertifications(faker, new Random().nextInt(10));
        String city = faker.address().city();
        String longitude = faker.address().longitude();
        String latitude = faker.address().latitude();
        Array certificationsArray = connection.createArrayOf("VARCHAR", certifications);
        PreparedStatement cvStatement = connection.prepareStatement(cvQuery);
        cvStatement.setString(1, cvId);
        cvStatement.setBoolean(2, true);
        cvStatement.setString(3, faker.job().title());
        cvStatement.setString(4, fileId);
        cvStatement.setString(5, city);
        cvStatement.setString(6, experience);
        cvStatement.setString(7, faker.job().field());
        cvStatement.setString(8, name);
        cvStatement.setString(9, faker.internet().emailAddress());
        cvStatement.setString(10, faker.phoneNumber().phoneNumber());
        cvStatement.setString(11, faker.text().text());
        cvStatement.setString(12, minSalary);
        cvStatement.setString(13, maxSalary);
        cvStatement.setString(14, faker.currency().code());
        cvStatement.setString(15, faker.company().name());
        cvStatement.setString(16, randomTeam);
        cvStatement.setArray(17, languagesArray);
        cvStatement.setString(18, randomJobType);
        cvStatement.setArray(19, skillsArray);
        cvStatement.setArray(20, educationsArray);
        cvStatement.setArray(21, certificationsArray);
        cvStatement.setTimestamp(22, new Timestamp(System.currentTimeMillis()));
        cvStatement.executeUpdate();

        feedLocation(connection, city, longitude, latitude);
        feedCVEmbedding(connection, cvId, longitude, latitude);

        // Get the list of skills
        Map<Integer, String> skillsMap = fetchSkills(connection);
        Map<Integer, String> randomPairsMap = skillsMap.entrySet()
                .stream()
                .skip(new Random().nextInt(skillsMap.size()))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        insertIntoMappingTable(uuid1, randomPairsMap, connection);

        System.out.println("CV data successfully inserted.");
    }

    private static void feedJD(Connection connection, Faker faker) throws SQLException {
        String schema = "searchnmatch";
        String tableName = "t_jd_detail";
        // Insert into t_file table
        String fileQuery = "INSERT INTO searchnmatch.t_file (file_id, s3_object_key, file_status, file_name, " +
                "file_type, uploaded_by, created_datetime) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        UUID uuid = UUID.randomUUID();
        String fileId = uuid.toString();
        String name = faker.name().firstName();
        String email = faker.internet().emailAddress();

        Random random = new Random();

        PreparedStatement fileStatement = connection.prepareStatement(fileQuery);
        fileStatement.setString(1, fileId);
        fileStatement.setString(2, "internal/jd/" + name + ".pdf");
        fileStatement.setObject(3, getRandomFileStatus(), Types.OTHER);
        fileStatement.setString(4, name + ".pdf");
        fileStatement.setObject(5, FILE_TYPE.JD, Types.OTHER);
        fileStatement.setString(6, email);
        fileStatement.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
        fileStatement.executeUpdate();


        // Get column names from the metadata
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columnsResultSet = metaData.getColumns(null, schema, tableName, "%");

        // Construct the list of column names
        StringBuilder columnNames = new StringBuilder();
        while (columnsResultSet.next()) {
            String columnName = columnsResultSet.getString("COLUMN_NAME");

            columnNames.append(columnName).append(", ");
        }

        // Remove the trailing comma and space
        if (columnNames.length() > 0) {
            columnNames.setLength(columnNames.length() - 2);
        }

        // Construct the insert query with dynamically obtained column names
        String insertQuery = String.format("INSERT INTO %s.%s (%s) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                        " ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                schema, tableName, columnNames);

        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
        // Set values for the placeholders
        UUID uuid1 = UUID.randomUUID();
        String jdID = uuid1.toString();
        boolean jdStatus = random.nextBoolean();
        String title = faker.job().title();
        String team = getRandomTeam();
        String domainName = faker.job().field();
        String summary = getRandomSummary(faker);
        String location = faker.address().cityName();
        String minSalary = roundToNearestHundred(faker.number().numberBetween(10000, 20000)) + "";
        String maxSalary = roundToNearestHundred(faker.number().numberBetween(20000, 1000000)) + "";
        String currency = faker.currency().code();
        String owner = faker.company().name();
        String clientCompany = faker.company().name();
        String[] languages = getRandomLanguages(random.nextInt(5) + 1);
        Array languagesArray = connection.createArrayOf("VARCHAR", languages);

        String[] education = getRandomEducations(faker, random.nextInt(10));
        Array educationArray = connection.createArrayOf("VARCHAR", education);
        String minExperience = faker.number().numberBetween(10, 200) + "";
        String maxExperience = faker.number().numberBetween(10, 200) + "";
        String jobType = getRandomJobType();
        String[] skills = getRandomSkills(faker, random.nextInt(30));
        Array skillsArray = connection.createArrayOf("VARCHAR", skills);

        ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of("UTC"));
        Timestamp createdDateTime = Timestamp.from(zonedDateTime.toInstant());

        String updatedBY = faker.internet().emailAddress();
        Timestamp updatedDateTime = Timestamp.from(zonedDateTime.toInstant());

        // Set values for the placeholders
        preparedStatement.setString(1, jdID);
        preparedStatement.setBoolean(2, jdStatus);
        preparedStatement.setString(3, title);
        preparedStatement.setString(4, fileId);
        preparedStatement.setString(5, team);
        preparedStatement.setString(6, domainName);
        preparedStatement.setString(7, summary);
        preparedStatement.setString(8, location);
        preparedStatement.setString(9, minSalary);
        preparedStatement.setString(10, maxSalary);
        preparedStatement.setString(11, currency);
        preparedStatement.setString(12, owner);
        preparedStatement.setString(13, clientCompany);
        preparedStatement.setArray(14, languagesArray);
        preparedStatement.setArray(15, educationArray);
        preparedStatement.setString(16, minExperience);
        preparedStatement.setString(17, maxExperience);
        preparedStatement.setString(18, jobType);
        preparedStatement.setArray(19, skillsArray);
        preparedStatement.setTimestamp(20, createdDateTime, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        preparedStatement.setString(21, updatedBY);
        preparedStatement.setTimestamp(22, updatedDateTime, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        preparedStatement.setObject(23, null);


        // Execute the insert statement
        preparedStatement.executeUpdate();
        String longitude = faker.address().longitude();
        String latitude = faker.address().latitude();
        feedJDEmbedding(connection, jdID, longitude, latitude);
        System.out.println("JD data successfully inserted.");
    }

    private static void feedJDEmbedding(Connection connection, String jdId, String lng, String lat) throws SQLException {

        double latitude = Double.parseDouble(lat); // Example latitude
        double longitude = Double.parseDouble(lng); // Example longitude
        // Insert into t_jd_embeddings table
        Double[] educationEmbedding = getEmbeddingArray();
        Double[] experiencEmbedding = getEmbeddingArray();
        String query = "INSERT INTO searchnmatch.t_jd_embeddings (jd_id, education, location,experience, " +
                "created_datetime) " +
                "VALUES (?, ?,  ST_SetSRID(ST_MakePoint(?, ?), 4326), ?,?)";

        PreparedStatement statement = connection.prepareStatement(query);
        statement.setString(1, jdId);
        statement.setArray(2, connection.createArrayOf("float8", educationEmbedding));
        statement.setDouble(3, longitude);
        statement.setDouble(4, latitude);
        statement.setArray(5, connection.createArrayOf("float8", experiencEmbedding));
        statement.setTimestamp(6, new Timestamp(System.currentTimeMillis()));

        statement.executeUpdate();

        System.out.println(" jd Embedding data successfully inserted." + jdId);
    }


    public static Object getRandomFileStatus() {
        // Define enum FILE_STATUS
        enum FILE_STATUS {
            NON_PROCESSED,
            PROCESSED_OK,
            LOGICAL_ERROR,
            RUNTIME_ERROR,
        }

        FILE_STATUS[] values = FILE_STATUS.values();
        Random random = new Random();
        return values[random.nextInt(values.length)];
    }

    public static String[] getRandomLanguages(int numberOfLanguages) {
        List<String> languages = Arrays.asList(
                "English", "Spanish", "French", "German", "Chinese", "Japanese", "Russian", "Arabic", "Italian");

        if (numberOfLanguages > languages.size()) {
            throw new IllegalArgumentException("Number of requested languages exceeds the available options.");
        }

        Random random = new Random();
        List<String> selectedLanguages = new ArrayList<>();

        while (selectedLanguages.size() < numberOfLanguages) {
            String randomLanguage = languages.get(random.nextInt(languages.size()));
            if (!selectedLanguages.contains(randomLanguage)) {
                selectedLanguages.add(randomLanguage);
            }
        }

        return selectedLanguages.toArray(new String[0]);
    }

    public static String getRandomTeam() {
        List<String> teamList = Arrays.asList("Team1", "Team2", "Team3");

        Random random = new Random();
        int randomIndex = random.nextInt(teamList.size());
        return teamList.get(randomIndex);
    }

    public static String[] getRandomSkills(Faker faker, int numberOfSkills) {
        List<String> skills = new ArrayList<>();

        for (int i = 0; i < numberOfSkills; i++) {
            skills.add(faker.job().keySkills());
        }

        return skills.toArray(new String[0]);
    }

    public static String getRandomJobType() {
        List<String> jobTypeList = Arrays.asList("PERMANENT", "CONTRACT", "OTHER");

        Random random = new Random();
        int randomIndex = random.nextInt(jobTypeList.size());
        return jobTypeList.get(randomIndex);
    }

    public static String[] getRandomCertifications(Faker faker, int numberOfCertifications) {
        List<String> certifications = new ArrayList<>();

        for (int i = 0; i < numberOfCertifications; i++) {
            certifications.add(faker.company().buzzword());
        }

        return certifications.toArray(new String[0]);
    }

    public static String[] getRandomEducations(Faker faker, int numberOfEducations) {
        List<String> educations = new ArrayList<>();

        for (int i = 0; i < numberOfEducations; i++) {
            educations.add(faker.educator().course());
        }

        return educations.toArray(new String[0]);
    }

    public static int roundToNearestHundred(int number) {
        return Math.round(number / 100.0f) * 100;
    }

    public static String getRandomSummary(Faker faker) {
        return faker.lorem().sentence();
    }

    private static void feedLocation(Connection connection, String city, String lng, String lat) throws SQLException {
        // Sample location data

        double latitude = Double.parseDouble(lat); // Example latitude
        double longitude = Double.parseDouble(lng); // Example longitude
        Timestamp createdDateTime = new Timestamp(System.currentTimeMillis());
        String query = "INSERT INTO searchnmatch.t_location_data (location, latitude, longitude, point, " +
                "created_datetime) " +
                "VALUES (?, ?, ?, ST_SetSRID(ST_MakePoint(?, ?), 4326), ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, city);
            preparedStatement.setDouble(2, latitude);
            preparedStatement.setDouble(3, longitude);
            preparedStatement.setDouble(4, longitude);
            preparedStatement.setDouble(5, latitude);
            preparedStatement.setTimestamp(6, createdDateTime);
            preparedStatement.executeUpdate();
        }
        System.out.println("Location data successfully inserted.");
    }

    private static void feedCVEmbedding(Connection connection, String cvId, String lng, String lat) throws SQLException {

        double latitude = Double.parseDouble(lat); // Example latitude
        double longitude = Double.parseDouble(lng); // Example longitude
        // Insert into t_cv_embeddings table
        Double[] educationEmbedding = getEmbeddingArray();
        Double[] experiencEmbedding = getEmbeddingArray();
        String query = "INSERT INTO searchnmatch.t_cv_embeddings (cv_id, education, location,experience, " +
                "created_datetime) " +
                "VALUES (?, ?,  ST_SetSRID(ST_MakePoint(?, ?), 4326),?, ?)";

        PreparedStatement cvStatement = connection.prepareStatement(query);
        cvStatement.setString(1, cvId);
        cvStatement.setArray(2, connection.createArrayOf("float8", educationEmbedding));
        cvStatement.setDouble(3, longitude);
        cvStatement.setDouble(4, latitude);
        cvStatement.setArray(5, connection.createArrayOf("float8", experiencEmbedding));
        cvStatement.setTimestamp(6, new Timestamp(System.currentTimeMillis()));

        cvStatement.executeUpdate();
        System.out.println(" cv Embedding data successfully inserted.");

    }

    private static void feedSkills(Connection connection) throws SQLException {

        String randomWord = generateRandomWord();
        insertIntoSkillsTable(connection, randomWord);

    }


    private static String generateRandomWord() {
        int wordLength = getRandomNumberInRange(10, 15); // Adjust the range as needed
        StringBuilder randomWord = new StringBuilder();

        for (int i = 0; i < wordLength; i++) {
            char randomChar = getRandomCharacter();
            randomWord.append(randomChar);
        }

        return randomWord.toString();
    }

    private static int getRandomNumberInRange(int min, int max) {
        Random random = new Random();
        return random.nextInt((max - min) + 1) + min;
    }

    private static char getRandomCharacter() {
        Random random = new Random();
        char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        int randomIndex = random.nextInt(alphabet.length);
        return alphabet[randomIndex];
    }


    public static Double[] getEmbeddingArray() {
        // Create a Random object
        Random random = new Random();

        // Define the array length
        int length = 1536;

        // Create an array of doubles to store random numbers
        Double[] randomArray = new Double[length];

        // Generate random numbers between -1 and 1
        for (int i = 0; i < length; i++) {
            randomArray[i] = random.nextDouble() * 2 - 1;
        }
        return randomArray;

    }

    private static void insertIntoSkillsTable(Connection connection, String skill_name) throws SQLException {

        String schema = "searchnmatch";
        String tableName = "t_skills";
        // Insert into t_file table
        String skillQuery = "INSERT INTO searchnmatch.t_skills (skill_name)" +
                "VALUES (?)";

        PreparedStatement fileStatement = connection.prepareStatement(skillQuery);
        fileStatement.setString(1, skill_name);

        fileStatement.executeUpdate();

    }

    private static Map<Integer, String> fetchSkills(Connection connection) throws SQLException {
        Map<Integer, String> skillsMap = new HashMap<>();
        String sql = "SELECT skill_id, skill_name FROM searchnmatch.t_skills LIMIT 100";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                Integer skillId = resultSet.getInt("skill_id");
                String skillName = resultSet.getString("skill_name");
                skillsMap.put(skillId, skillName);
            }
        }
        return skillsMap;
    }

    private static void insertIntoMappingTable(UUID uuid1, Map<Integer, String> randomPairsMap,
                                               Connection connection) throws SQLException {
        String query = "INSERT INTO searchnmatch.t_cv_skills (cv_id, skill_id) " +
                "VALUES (?, ?)";
        PreparedStatement cvStatement = connection.prepareStatement(query);
        for (Map.Entry<Integer, String> entry : randomPairsMap.entrySet()) {
            int skillId = entry.getKey();
            cvStatement.setString(1, uuid1.toString());
            cvStatement.setInt(2, skillId);
            cvStatement.executeUpdate();
        }
    }

    private static void feedSkillsEmbedding(Connection connection) throws SQLException {
        // SQL query to insert a new row into the t_skills_embedding table
        String sql = "INSERT INTO searchnmatch.t_skills_embedding (skill_id, skill, value, created_datetime) VALUES " +
                "(?, ?, ?, ?)";
        String skillId = UUID.randomUUID().toString();
        Double[] skillsEmbedding = getEmbeddingArray();
        String skill_name = generateRandomWord();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            // Set the parameters for the PreparedStatement
            pstmt.setString(1, skillId);
            pstmt.setString(2, skill_name);
            // Set the vector value - replace with your actual vector value
            pstmt.setObject(3, connection.createArrayOf("float8", skillsEmbedding));
            pstmt.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now())); // Set created_datetime


            // Execute the INSERT statement
            int rowsInserted = pstmt.executeUpdate();
            if (rowsInserted > 0) {
                System.out.println("A new row has been inserted into t_skills_embedding.");
            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void feedCustomMatchData(Connection connection, Faker faker) throws SQLException {
        Random random = new Random();
        // First feed entries to custom_match_metric table
        feedCustomMatchMetricData(connection, random);
        //Delete everything
        deleteRecords("searchnmatch", "t_custom_match_metric_result", connection);
        deleteRecords("searchnmatch", "t_custom_match_history", connection);

        // First fetch the set of cv id's and jd id's to form a mapping between them
        // Consider the 10k records from each
        List<String> cvIds = fetchIds(connection, "searchnmatch.t_cv_detail", "cv_id", 10000);
        List<String> jdIds = fetchIds(connection, "searchnmatch.t_jd_detail", "jd_id", 10000);


        String feedMatchHistoryTable = "INSERT INTO searchnmatch.t_custom_match_history (custom_match_id, match_type," +
                " " +
                "jd_id, cv_id, custom_metric_id, overall_result, created_by, created_datetime, email_content) VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String feedMatchMetricResultTable = "INSERT INTO searchnmatch.t_custom_match_metric_result (custom_match_id, " +
                "skills, " +
                "education, experience, location) VALUES (?, ?, ?, ?, ?)";

        String feedFeedbackTable = "INSERT INTO searchnmatch.t_feedback (match_id, match_type, feedback, created_by, " +
                "created_datetime, updated_by, updated_datetime) VALUES (?, ?, ?, ?, ?, ?, ?)";


        PreparedStatement matchHistoryTableStatement = connection.prepareStatement(feedMatchHistoryTable);
        PreparedStatement matchMetricResultTableStatement = connection.prepareStatement(feedMatchMetricResultTable);
        PreparedStatement feedbackTableStatement = connection.prepareStatement(feedFeedbackTable);

        // Randomly select match_type (CV_TO_JD or JD_TO_CV)
        MatchType[] matchTypes = {MatchType.CV_TO_JD, MatchType.JD_TO_CV};
        // Get list of all custom metric id from DB
        // Create a prepared statement to fetch custom_metric_id from the table
        PreparedStatement statement = connection.prepareStatement("SELECT custom_metric_id FROM searchnmatch" +
                ".t_custom_match_metric");

        // Execute the query to fetch custom_metric_id
        ResultSet resultSet = statement.executeQuery();

        // Store the custom_metric_ids in a list
        List<String> customMetricIds = new ArrayList<>();
        while (resultSet.next()) {
            customMetricIds.add(resultSet.getString("custom_metric_id"));
        }

        // Lets have 10 Lakh of data in the match table. so loop 10 lakh times
        for (int i = 0; i < 1000000; i++) {
            // Retrieve the custom_metric_id at the random index
            String customMetricId = customMetricIds.get(random.nextInt(customMetricIds.size()));
            // Insert data to t_custom_match_history with random custom_metric_id
            // Generate UUID for custom_match_id
            String customMatchId = UUID.randomUUID().toString();
            MatchType matchType = matchTypes[random.nextInt(matchTypes.length)];
            String jdId = jdIds.get(random.nextInt(10000));
            String cvId = jdIds.get(random.nextInt(10000));
            // Generate random overall_result (0 to 100)
            int overallResult = random.nextInt(101);
            String createdBy = faker.internet().emailAddress();
            // Generate random created_datetime string (formatted as 'yyyy-MM-dd HH:mm:ss')
            Timestamp createdDatetime = new Timestamp(System.currentTimeMillis());
            // Generate random email_content string
            String emailContent = "Random email content for custom match";
            // Set values for the prepared statement
            matchHistoryTableStatement.setObject(1, customMatchId); // custom_match_id
            matchHistoryTableStatement.setObject(2, matchType, Types.OTHER); // match_type
            matchHistoryTableStatement.setString(3, jdId); // jd_id
            matchHistoryTableStatement.setString(4, cvId); // cv_id
            matchHistoryTableStatement.setObject(5, customMetricId); // custom_metric_id
            matchHistoryTableStatement.setInt(6, overallResult); // overall_result
            matchHistoryTableStatement.setString(7, createdBy); // created_by
            matchHistoryTableStatement.setTimestamp(8, createdDatetime); // created_datetime
            matchHistoryTableStatement.setString(9, emailContent); // email_content
            // Execute the insert statement
            matchHistoryTableStatement.executeUpdate();

            // Insert data to t_custom_match_metric_result with this custom_match_id
            int skills = random.nextInt(101);
            int education = random.nextInt(101);
            int location = random.nextInt(101);
            int experience = random.nextInt(101);

            matchMetricResultTableStatement.setObject(1, customMatchId);
            matchMetricResultTableStatement.setInt(2, skills);
            matchMetricResultTableStatement.setInt(3, education);
            matchMetricResultTableStatement.setInt(4, experience);
            matchMetricResultTableStatement.setInt(5, location);

            matchMetricResultTableStatement.executeUpdate();

            feedbackTableStatement.setObject(1, customMatchId);
            feedbackTableStatement.setObject(2, matchType, Types.OTHER); // match_type
            feedbackTableStatement.setBoolean(3, random.nextBoolean()); // Example feedback value, replace with 
            // actual value
            feedbackTableStatement.setString(4, createdBy);
            feedbackTableStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now())); // Set created_datetime
            feedbackTableStatement.setString(6, null); // Set updated_by (can be null initially)
            feedbackTableStatement.setTimestamp(7, null); // Set updated_datetime (can be null initially)


            feedbackTableStatement.executeUpdate();
            // Execute the insert statement

            connection.commit();
            System.out.println(">>>>>>>>>>>>>>>>>>>INSERTED " + (i + 1) + " ROWS<<<<<<<<<<<<<<<<<<");


        }
    }

    private static void feedCustomMatchMetricData(Connection connection, Random random) throws SQLException {
        // Generate random weightages (1, 50, or 100)
        int[] weightages = {1, 50, 100};
        // Generate random location radius (10, 100, 200, or 500)
        int[] locationRadii = {10, 100, 200, 500};
        deleteRecords("searchnmatch", "t_custom_match_metric", connection);
        // First feed data to custom metric table
        String feedMatchMetricTable = "INSERT INTO searchnmatch.t_custom_match_metric (custom_metric_id, " +
                "skills_weightage, " +
                "education_weightage, experience_weightage, location_weightage, location_radius) VALUES (?, ?, ?, ?, " +
                "?, ?)";
        PreparedStatement matchMetricTableStatement = connection.prepareStatement(feedMatchMetricTable);

        List<int[]> combinations = new ArrayList<>();
        for (int skillsWeightage : weightages) {
            for (int educationWeightage : weightages) {
                for (int experienceWeightage : weightages) {
                    for (int locationWeightage : weightages) {
                        for (int locationRadius : locationRadii) {
                            combinations.add(new int[]{skillsWeightage, educationWeightage, experienceWeightage,
                                    locationWeightage, locationRadius});
                        }
                    }
                }
            }
        }
        for (int i = 0; i < 324; i++) {
            int[] combination = combinations.get(i);
            String customMetricId = UUID.randomUUID().toString();
            double skillsWeightage = combination[0];
            double educationWeightage = combination[1];
            double experienceWeightage = combination[2];
            double locationWeightage = combination[3];
            double locationRadius = combination[4];
            // Insert the combination into the database
            // Set values for the prepared statement
            matchMetricTableStatement.setString(1, customMetricId); // custom_metric_id
            matchMetricTableStatement.setDouble(2, skillsWeightage); // skills_weightage
            matchMetricTableStatement.setDouble(3, educationWeightage); // education_weightage
            matchMetricTableStatement.setDouble(4, experienceWeightage); // experience_weightage
            matchMetricTableStatement.setDouble(5, locationWeightage); // location_weightage
            matchMetricTableStatement.setDouble(6, locationRadius); // location_radius
            matchMetricTableStatement.executeUpdate();
            connection.commit();
        }
    }


    public static void deleteRecords(String schema, String tableName, Connection connection) {
        String deleteQuery = String.format("DELETE FROM %s.%s", schema, tableName);
        try (
                PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery);
        ) {
            // Execute the delete statement
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println("Deleted " + rowsAffected + " records from " + schema + "." + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static List<String> fetchIds(Connection connection, String tableName, String idColumnName, int limit) throws SQLException {
        List<String> ids = new ArrayList<>();
        String sql = "SELECT " + idColumnName + " FROM " + tableName + " LIMIT ?";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, limit);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    ids.add(resultSet.getString(idColumnName));
                }
            }
        }
        return ids;
    }

    private static void feedskillsMapping(Connection connection) throws SQLException {

        Set<String> skillsToInsert = fetchSkillsFromTable(connection);
        for (String skill1 : skillsToInsert) {
            for (String skill2 : skillsToInsert) {
                if (!skill1.equals(skill2)) { // Avoid inserting the same skill combination
                    insertSkillMapping(connection, skill1, skill2);
                }
            }
        }

    }

    private static Set<String> fetchSkillsFromTable(Connection connection) {
        Set<String> skills = new HashSet<>();
        String sql = "SELECT skill FROM searchnmatch.t_skills_embedding";

        try (
                PreparedStatement pstmt = connection.prepareStatement(sql);
                ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String skill = rs.getString("skill");
                skills.add(skill);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return skills;
    }

    private static void insertSkillMapping(Connection connection, String skill1, String skill2) throws SQLException {


        // SQL query to check if the skill combination already exists
        String selectSql = "SELECT COUNT(*) FROM searchnmatch.t_skills_mapping WHERE (skill1 = ? AND skill2 = ?) OR " +
                "(skill1 = ? AND skill2 = ?)";

        // SQL query to insert data
        String insertSql = "INSERT INTO searchnmatch.t_skills_mapping " +
                "(skill_mapping_id, skill1, skill2, match_score, created_datetime, updated_by, updated_datetime) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (
                // Checking if the skill combination already exists
                PreparedStatement selectStmt = connection.prepareStatement(selectSql);
                PreparedStatement insertStmt = connection.prepareStatement(insertSql);
        ) {
            selectStmt.setString(1, skill1);
            selectStmt.setString(2, skill2);
            selectStmt.setString(3, skill2);
            selectStmt.setString(4, skill1);
            ResultSet resultSet = selectStmt.executeQuery();

            resultSet.next();
            int count = resultSet.getInt(1);

            // If the skill combination doesn't exist, insert it
            if (count == 0) {
                // Generating a unique ID for skill_mapping_id
                String skillMappingId = UUID.randomUUID().toString();
                Random r = new Random();
                Double score = Math.round((0 + 100 * r.nextDouble()) * 100.0) / 100.0;

                // Setting values for the prepared statement
                insertStmt.setString(1, skillMappingId);
                insertStmt.setString(2, skill1);
                insertStmt.setString(3, skill2);
                insertStmt.setDouble(4, score); // Example match score
                insertStmt.setTimestamp(5, new Timestamp(System.currentTimeMillis())); // Current timestamp for 
                // created_datetime
                insertStmt.setString(6, "admin"); // Example updated_by
                insertStmt.setTimestamp(7, null); // null for updated_datetime

                // Executing the insert statement
                insertStmt.executeUpdate();

                System.out.println("Inserted: " + skill1 + " - " + skill2);
            } else {
                System.out.println("Skipping duplicate: " + skill1 + " - " + skill2);
            }
        }

    }

    public static void modifyExistingCVTable(Connection connection) throws SQLException {
        String sql = "SELECT cv_id, skills FROM searchnmatch.t_cv_detail";
        String matchingSQL = "SELECT s.skill_name FROM searchnmatch.t_cv_skills cvs JOIN " +
                "searchnmatch.t_cv_detail cd ON cvs.cv_id = cd.cv_id JOIN searchnmatch.t_skills s ON cvs.skill_id = s" +
                ".skill_id WHERE cd.cv_id = ?";
        String updateCVSkillsSQL = "UPDATE searchnmatch.t_cv_detail SET skills = ? WHERE cv_id = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        PreparedStatement preparedStatement1 = connection.prepareStatement(matchingSQL);
        PreparedStatement preparedStatement2 = connection.prepareStatement(updateCVSkillsSQL);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            List<String> newSkillNames = new ArrayList<>();
            String cvId = resultSet.getString("cv_id");
//            String[] skillsArray = (String[])resultSet.getArray("skills").getArray();
            // Find the matching data from cv_skills
            preparedStatement1.setString(1, cvId);
            ResultSet resultSet1 = preparedStatement1.executeQuery();
            while (resultSet1.next()) {
                String skillName = resultSet1.getString("skill_name");
                newSkillNames.add(skillName);
            }
            preparedStatement2.setArray(1, connection.createArrayOf("text", newSkillNames.toArray(new String[0])));
            preparedStatement2.setString(2, cvId);
            preparedStatement2.executeUpdate();
        }
    }

    enum FILE_TYPE {
        CV,
        JD
    }

    public enum MatchType {
        CV_TO_JD,
        JD_TO_CV
    }

}
