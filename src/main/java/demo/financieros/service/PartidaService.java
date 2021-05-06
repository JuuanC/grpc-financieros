package demo.financieros.service;

import io.grpc.stub.StreamObserver;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import ssaver.gob.mx.financieros.catalogos.*;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PartidaService extends PartidaMethodGrpc.PartidaMethodImplBase {

    @Inject
    PgPool client;

    @Override
    public void getPartidas(EmptyGRPC request, StreamObserver<listPartidaGRPC> responseObserver) {
        client.query("SELECT * FROM catalogo.partida").execute(event -> {
            if (event.succeeded()){
                RowSet<Row> rows = event.result();

                listPartidaGRPC.Builder getPartidas = listPartidaGRPC.newBuilder();

                rows.forEach(row ->{
                    PartidaGRPC partidaGRPC = PartidaGRPC.newBuilder()
                            .setIdPartida(row.getInteger("id_partida"))
                            .setClave(row.getInteger("clave"))
                            .setDescripcion(row.getString("descripcion"))
                            .setUsuario(row.getString("usuario"))
                            .build();

                    getPartidas.addPartidas(partidaGRPC);
                });
                responseObserver.onNext(getPartidas.build());
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void getPartidaById(idPartidaGRPC request, StreamObserver<PartidaGRPC> responseObserver) {
        client.preparedQuery("SELECT * FROM catalogo.partida WHERE id_partida = $1")
                .execute(Tuple.of(request.getIdPartida()), event ->{
            if(event.succeeded()){
                RowSet<Row> rows = event.result();
                PartidaGRPC.Builder partidaGRPC = PartidaGRPC.newBuilder();

                rows.forEach( row -> {
                    partidaGRPC
                            .setIdPartida(row.getInteger("id_partida"))
                            .setClave(row.getInteger("clave"))
                            .setDescripcion(row.getString("descripcion"))
                            .setUsuario(row.getString("usuario"))
                            .build();
                });
                responseObserver.onNext(partidaGRPC.build());
                responseObserver.onCompleted();
            }
        });
    }
}
