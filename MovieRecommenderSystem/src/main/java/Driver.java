/**
 * Created by orange475 on 4/21/17.
 */
public class Driver {
    public static void main(String[] args) throws Exception {

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        Multiplication multiplication = new Multiplication();
        RecommenderListGenerator recommenderListGenerator = new RecommenderListGenerator();

        String rawInput = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMatrixDir = args[2];
        String multiplicationDir = args[3];
        String fileNameDir = args[4];
        String recommenderDir = args[5];
        String[] path1 = {rawInput, userMovieListOutputDir};
        String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
        String[] path3 = {coOccurrenceMatrixDir, rawInput, multiplicationDir};
        String[] path4 = {rawInput,fileNameDir, multiplicationDir,recommenderDir};

        dataDividerByUser.main(path1);
        coOccurrenceMatrixGenerator.main(path2);

        multiplication.main(path3);
        recommenderListGenerator.main(path4);
    }

}