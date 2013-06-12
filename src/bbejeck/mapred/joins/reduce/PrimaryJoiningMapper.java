package bbejeck.mapred.joins.reduce;

/**
 * User: Bill Bejeck
 * Date: 6/8/13
 * Time: 10:12 PM
 */
public class PrimaryJoiningMapper extends BaseJoiningMapper {

    public PrimaryJoiningMapper() {
    }

    @Override
    public int getJoinOrder() {
        return 1;
    }
}
