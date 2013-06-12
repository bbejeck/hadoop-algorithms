package bbejeck.mapred.joins.reduce;

/**
 * User: Bill Bejeck
 * Date: 6/8/13
 * Time: 10:12 PM
 */
public class SecondaryJoiningMapper extends BaseJoiningMapper {

    public SecondaryJoiningMapper() {
    }

    @Override
    public int getJoinOrder() {
        return 2;
    }
}
