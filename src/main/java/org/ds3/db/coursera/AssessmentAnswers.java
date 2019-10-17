/*
 * The MIT License
 *
 * Copyright 2019 NYU (Heiko Mueller <heiko.mueller@nyu.edu>).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.ds3.db.coursera;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ds3.db.JDBCConnector;

/**
 * Count answers to all questions of a given type. For questions of type
 * 'reflect' (assessment_question_type_id = 7) the different answer texts
 * are also printed to standard output.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class AssessmentAnswers {
    
    private static final Logger LOGGER = Logger
            .getLogger(AssessmentAnswers.class.getName());
    
    public void run(int questionType, PrintWriter out) throws java.sql.SQLException {
    
        String sqlOuter =
                "SELECT aq.assessment_question_id, aq.assessment_question_prompt, COUNT(*) " +
                "FROM assessment_questions aq, assessment_responses ar " +
                "WHERE aq.assessment_question_id = ar.assessment_question_id " +
                "AND aq.assessment_question_type_id = ? " +
                "GROUP BY aq.assessment_question_id, aq.assessment_question_prompt";
        
        String sqlReflec = 
                "SELECT p.assessment_response_answer, COUNT(*) " +
                "FROM assessment_response_patterns p, assessment_responses r " +
                "WHERE p.assessment_response_id = r.assessment_response_id AND " +
                "r.assessment_question_id = ? " +
                "GROUP BY p.assessment_response_answer " +
                "ORDER BY COUNT(*) DESC;";
        try (
                Connection con = JDBCConnector.connect();
                PreparedStatement pSqlOuter = con.prepareStatement(sqlOuter);
                PreparedStatement pSqlReflect = con.prepareStatement(sqlReflec);
        ) {
            pSqlOuter.setInt(1, questionType);
            try (ResultSet rs = pSqlOuter.executeQuery()) {
                while (rs.next()) {
                    String questionId = rs.getString(1);
                    JsonObject obj = JsonParser
                            .parseString(rs.getString(2))
                            .getAsJsonObject();
                    String prompt = obj
                            .get("definition")
                            .getAsJsonObject()
                            .get("value")
                            .getAsString();
                    int count = rs.getInt(3);
                    out.println("Question: " + CouseraHelper.parsePrompt(prompt) + " (" + count + " responses)");
                    if (questionType == 7) {
                        out.println();
                        pSqlReflect.setString(1, questionId);
                        try (ResultSet rsReflect = pSqlReflect.executeQuery()) {
                            while (rsReflect.next()) {
                                String answer = rsReflect.getString(1);
                                int answerCount = rsReflect.getInt(2);
                                out.println("- " + answerCount + "x: " + answer);
                            }
                        }
                        out.println();
                        out.println();
                        out.println();
                    }
                }
            }
        }
    }
    
    private static final String COMMAND = 
            "Usage:\n" +
            "  <question-type> [1:checkbox | 5:mcq | 7:reflect]";
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        int questionType = Integer.parseInt(args[0]);
        
        try (PrintWriter out = new PrintWriter(System.out)) {
            new AssessmentAnswers().run(questionType, out);
        } catch (java.sql.SQLException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
