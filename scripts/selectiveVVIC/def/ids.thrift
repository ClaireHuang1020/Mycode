include "exceptions.thrift"

service IdsService {
    i64 getId(
        1:string target
    ) throws (
        1:exceptions.InvalidOperationException ioe,
        2:exceptions.InternalException ie
    )

    i64 reset(
        1:string target
    ) throws (
        1:exceptions.InvalidOperationException ioe,
        2:exceptions.InternalException ie
    )

}
