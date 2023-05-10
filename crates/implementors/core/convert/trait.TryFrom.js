(function() {var implementors = {
"mysten_network":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/string/struct.String.html\" title=\"struct alloc::string::String\">String</a>&gt; for <a class=\"struct\" href=\"mysten_network/multiaddr/struct.Multiaddr.html\" title=\"struct mysten_network::multiaddr::Multiaddr\">Multiaddr</a>"],["impl&lt;'a&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;'a <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>&gt; for <a class=\"struct\" href=\"mysten_network/multiaddr/struct.Multiaddr.html\" title=\"struct mysten_network::multiaddr::Multiaddr\">Multiaddr</a>"]],
"narwhal_types":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"narwhal_types/struct.CertificateDigestProto.html\" title=\"struct narwhal_types::CertificateDigestProto\">CertificateDigest</a>&gt; for <a class=\"struct\" href=\"narwhal_types/struct.CertificateDigest.html\" title=\"struct narwhal_types::CertificateDigest\">CertificateDigest</a>"]],
"shared_crypto":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>&gt; for <a class=\"enum\" href=\"shared_crypto/intent/enum.IntentScope.html\" title=\"enum shared_crypto::intent::IntentScope\">IntentScope</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>&gt; for <a class=\"enum\" href=\"shared_crypto/intent/enum.IntentVersion.html\" title=\"enum shared_crypto::intent::IntentVersion\">IntentVersion</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>&gt; for <a class=\"enum\" href=\"shared_crypto/intent/enum.AppId.html\" title=\"enum shared_crypto::intent::AppId\">AppId</a>"]],
"sui_benchmark":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"enum\" href=\"sui_benchmark/workloads/adversarial/enum.AdversarialPayloadType.html\" title=\"enum sui_benchmark::workloads::adversarial::AdversarialPayloadType\">AdversarialPayloadType</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>&gt; for <a class=\"enum\" href=\"sui_benchmark/options/enum.RunSpec.html\" title=\"enum sui_benchmark::options::RunSpec\">RunSpec</a>"]],
"sui_indexer":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;SuiTransactionBlockResponse&gt; for <a class=\"struct\" href=\"sui_indexer/types/struct.CheckpointTransactionBlockResponse.html\" title=\"struct sui_indexer::types::CheckpointTransactionBlockResponse\">CheckpointTransactionBlockResponse</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;SuiTransactionBlockResponse&gt; for <a class=\"struct\" href=\"sui_indexer/types/struct.FastPathTransactionBlockResponse.html\" title=\"struct sui_indexer::types::FastPathTransactionBlockResponse\">FastPathTransactionBlockResponse</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"sui_indexer/types/struct.TemporaryTransactionBlockResponseStore.html\" title=\"struct sui_indexer::types::TemporaryTransactionBlockResponseStore\">TemporaryTransactionBlockResponseStore</a>&gt; for <a class=\"struct\" href=\"sui_indexer/models/transactions/struct.Transaction.html\" title=\"struct sui_indexer::models::transactions::Transaction\">Transaction</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"sui_indexer/models/objects/struct.Object.html\" title=\"struct sui_indexer::models::objects::Object\">Object</a>&gt; for Object"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"sui_indexer/models/system_state/struct.DBValidatorSummary.html\" title=\"struct sui_indexer::models::system_state::DBValidatorSummary\">DBValidatorSummary</a>&gt; for SuiValidatorSummary"]],
"sui_json_rpc_types":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;((ObjectID, SequenceNumber, ObjectDigest), Object, <a class=\"enum\" href=\"https://doc.rust-lang.org/nightly/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;MoveStructLayout&gt;, <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectDataOptions.html\" title=\"struct sui_json_rpc_types::SuiObjectDataOptions\">SuiObjectDataOptions</a>)&gt; for <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectData.html\" title=\"struct sui_json_rpc_types::SuiObjectData\">SuiObjectData</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;(ObjectInfo, <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectDataOptions.html\" title=\"struct sui_json_rpc_types::SuiObjectDataOptions\">SuiObjectDataOptions</a>)&gt; for <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectResponse.html\" title=\"struct sui_json_rpc_types::SuiObjectResponse\">SuiObjectResponse</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"enum\" href=\"sui_json_rpc_types/enum.SuiMoveStruct.html\" title=\"enum sui_json_rpc_types::SuiMoveStruct\">SuiMoveStruct</a>&gt; for GasCoin"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;TransactionEffects&gt; for <a class=\"enum\" href=\"sui_json_rpc_types/enum.SuiTransactionBlockEffects.html\" title=\"enum sui_json_rpc_types::SuiTransactionBlockEffects\">SuiTransactionBlockEffects</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectData.html\" title=\"struct sui_json_rpc_types::SuiObjectData\">SuiObjectData</a>&gt; for GasCoin"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;(ObjectRead, <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectDataOptions.html\" title=\"struct sui_json_rpc_types::SuiObjectDataOptions\">SuiObjectDataOptions</a>)&gt; for <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectResponse.html\" title=\"struct sui_json_rpc_types::SuiObjectResponse\">SuiObjectResponse</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;Object&gt; for <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiCoinMetadata.html\" title=\"struct sui_json_rpc_types::SuiCoinMetadata\">SuiCoinMetadata</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectResponse.html\" title=\"struct sui_json_rpc_types::SuiObjectResponse\">SuiObjectResponse</a>&gt; for ObjectInfo"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;((ObjectID, SequenceNumber, ObjectDigest), Object, <a class=\"enum\" href=\"https://doc.rust-lang.org/nightly/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;MoveStructLayout&gt;, <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectDataOptions.html\" title=\"struct sui_json_rpc_types::SuiObjectDataOptions\">SuiObjectDataOptions</a>, <a class=\"enum\" href=\"https://doc.rust-lang.org/nightly/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;<a class=\"struct\" href=\"sui_json_rpc_types/struct.DisplayFieldsResponse.html\" title=\"struct sui_json_rpc_types::DisplayFieldsResponse\">DisplayFieldsResponse</a>&gt;)&gt; for <a class=\"struct\" href=\"sui_json_rpc_types/struct.SuiObjectData.html\" title=\"struct sui_json_rpc_types::SuiObjectData\">SuiObjectData</a>"]],
"sui_rosetta":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;SuiTransactionBlockData&gt; for <a class=\"struct\" href=\"sui_rosetta/operations/struct.Operations.html\" title=\"struct sui_rosetta::operations::Operations\">Operations</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;TransactionData&gt; for <a class=\"struct\" href=\"sui_rosetta/operations/struct.Operations.html\" title=\"struct sui_rosetta::operations::Operations\">Operations</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>&gt; for <a class=\"enum\" href=\"sui_rosetta/types/enum.SuiEnv.html\" title=\"enum sui_rosetta::types::SuiEnv\">SuiEnv</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;SuiTransactionBlockResponse&gt; for <a class=\"struct\" href=\"sui_rosetta/operations/struct.Operations.html\" title=\"struct sui_rosetta::operations::Operations\">Operations</a>"]],
"sui_rpc_loadgen":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>&gt; for <a class=\"enum\" href=\"sui_rpc_loadgen/payload/enum.AddressQueryType.html\" title=\"enum sui_rpc_loadgen::payload::AddressQueryType\">AddressQueryType</a>"]],
"sui_tool":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>&gt; for <a class=\"enum\" href=\"sui_tool/db_tool/db_dump/enum.StoreName.html\" title=\"enum sui_tool::db_tool::db_dump::StoreName\">StoreName</a>"]],
"sui_types":[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>]&gt; for <a class=\"struct\" href=\"sui_types/base_types/struct.SuiAddress.html\" title=\"struct sui_types::base_types::SuiAddress\">SuiAddress</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"struct\" href=\"sui_types/object/struct.Object.html\" title=\"struct sui_types::object::Object\">Object</a>&gt; for <a class=\"struct\" href=\"sui_types/governance/struct.StakedSui.html\" title=\"struct sui_types::governance::StakedSui\">StakedSui</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/vec/struct.Vec.html\" title=\"struct alloc::vec::Vec\">Vec</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>, <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/alloc/struct.Global.html\" title=\"struct alloc::alloc::Global\">Global</a>&gt;&gt; for <a class=\"struct\" href=\"sui_types/base_types/struct.SuiAddress.html\" title=\"struct sui_types::base_types::SuiAddress\">SuiAddress</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>]&gt; for <a class=\"struct\" href=\"sui_types/digests/struct.ObjectDigest.html\" title=\"struct sui_types::digests::ObjectDigest\">ObjectDigest</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"enum\" href=\"sui_types/signature/enum.GenericSignature.html\" title=\"enum sui_types::signature::GenericSignature\">GenericSignature</a>&gt; for <a class=\"struct\" href=\"sui_types/base_types/struct.SuiAddress.html\" title=\"struct sui_types::base_types::SuiAddress\">SuiAddress</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>]&gt; for <a class=\"struct\" href=\"sui_types/base_types/struct.ObjectID.html\" title=\"struct sui_types::base_types::ObjectID\">ObjectID</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>]&gt; for <a class=\"struct\" href=\"sui_types/digests/struct.TransactionDigest.html\" title=\"struct sui_types::digests::TransactionDigest\">TransactionDigest</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"sui_types/object/struct.Object.html\" title=\"struct sui_types::object::Object\">Object</a>&gt; for <a class=\"struct\" href=\"sui_types/coin/struct.CoinMetadata.html\" title=\"struct sui_types::coin::CoinMetadata\">CoinMetadata</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"enum\" href=\"sui_types/base_types/enum.ObjectType.html\" title=\"enum sui_types::base_types::ObjectType\">ObjectType</a>&gt; for StructTag"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/string/struct.String.html\" title=\"struct alloc::string::String\">String</a>&gt; for <a class=\"struct\" href=\"sui_types/event/struct.EventID.html\" title=\"struct sui_types::event::EventID\">EventID</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"struct\" href=\"sui_types/crypto/struct.SuiAuthorityStrongQuorumSignInfo.html\" title=\"struct sui_types::crypto::SuiAuthorityStrongQuorumSignInfo\">SuiAuthorityStrongQuorumSignInfo</a>&gt; for <a class=\"type\" href=\"sui_types/crypto/type.AuthorityStrongQuorumSignInfo.html\" title=\"type sui_types::crypto::AuthorityStrongQuorumSignInfo\">AuthorityStrongQuorumSignInfo</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"struct\" href=\"sui_types/object/struct.MoveObject.html\" title=\"struct sui_types::object::MoveObject\">MoveObject</a>&gt; for <a class=\"struct\" href=\"sui_types/gas_coin/struct.GasCoin.html\" title=\"struct sui_types::gas_coin::GasCoin\">GasCoin</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.str.html\">str</a>&gt; for <a class=\"enum\" href=\"sui_types/crypto/enum.SignatureScheme.html\" title=\"enum sui_types::crypto::SignatureScheme\">SignatureScheme</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"enum\" href=\"sui_types/error/enum.SuiError.html\" title=\"enum sui_types::error::SuiError\">SuiError</a>&gt; for <a class=\"enum\" href=\"sui_types/error/enum.UserInputError.html\" title=\"enum sui_types::error::UserInputError\">UserInputError</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"struct\" href=\"sui_types/object/struct.Object.html\" title=\"struct sui_types::object::Object\">Object</a>&gt; for <a class=\"struct\" href=\"sui_types/gas_coin/struct.GasCoin.html\" title=\"struct sui_types::gas_coin::GasCoin\">GasCoin</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/vec/struct.Vec.html\" title=\"struct alloc::vec::Vec\">Vec</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>, <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/alloc/struct.Global.html\" title=\"struct alloc::alloc::Global\">Global</a>&gt;&gt; for <a class=\"struct\" href=\"sui_types/base_types/struct.ObjectID.html\" title=\"struct sui_types::base_types::ObjectID\">ObjectID</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;&amp;<a class=\"struct\" href=\"sui_types/object/struct.Object.html\" title=\"struct sui_types::object::Object\">Object</a>&gt; for <a class=\"struct\" href=\"sui_types/coin/struct.CoinMetadata.html\" title=\"struct sui_types::coin::CoinMetadata\">CoinMetadata</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"struct\" href=\"sui_types/crypto/struct.AuthorityPublicKeyBytes.html\" title=\"struct sui_types::crypto::AuthorityPublicKeyBytes\">AuthorityPublicKeyBytes</a>&gt; for <a class=\"type\" href=\"sui_types/crypto/type.AuthorityPublicKey.html\" title=\"type sui_types::crypto::AuthorityPublicKey\">AuthorityPublicKey</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.TryFrom.html\" title=\"trait core::convert::TryFrom\">TryFrom</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>&gt; for <a class=\"enum\" href=\"sui_types/move_package/enum.UpgradePolicy.html\" title=\"enum sui_types::move_package::UpgradePolicy\">UpgradePolicy</a>"]]
};if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()